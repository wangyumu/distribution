package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"github.com/docker/distribution"
	"github.com/docker/distribution/configuration"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/client"
	"github.com/docker/distribution/registry/client/auth"
	"github.com/docker/distribution/registry/client/auth/challenge"
	"github.com/docker/distribution/registry/client/transport"
	"github.com/docker/distribution/registry/proxy/scheduler"
	"github.com/docker/distribution/registry/storage"
	"github.com/docker/distribution/registry/storage/driver"
	"strings"
)

// proxyingRegistry fetches content from a remote registry and caches it locally
type proxyingRegistry struct {
	embedded       distribution.Namespace // provides local registry functionality
	scheduler      *scheduler.TTLExpirationScheduler
	remoteURL      url.URL
	remoteURLAll	[]url.URL
	urlPrefixAll	[]string
	authChallenger authChallenger
}

func MatchProjectPrefix(ctx context.Context, urlPrefixAll []string) []int {
	matched := []int{}
	fullURL, _ := ctx.Value("http.request.uri").(string)
	projectName := strings.Split( fullURL, "/")[2]

	for i := range urlPrefixAll {
		prefix := urlPrefixAll[i]
		for _, prefixOnce := range strings.Split(prefix, "|") {
			if prefixOnce == "*" {
				matched = append( matched, i)
			} else if strings.HasSuffix(prefixOnce, "*") &&
				strings.HasPrefix(projectName, strings.ReplaceAll(prefixOnce,"*", "")) {
				matched = append( matched, i)
			} else if strings.HasPrefix(prefixOnce, "*") &&
				strings.HasSuffix(projectName, strings.ReplaceAll(prefixOnce,"*", "")) {
				matched = append( matched, i)
			} else if strings.EqualFold(prefixOnce,projectName) {
				matched = append( matched, i)
			}
		}
	}

	if len(matched) == 0 {
		dcontext.GetLogger(ctx).Warnf("No Prefix Matched: %s , %s ", projectName, urlPrefixAll)
	}

	return matched
}

// NewRegistryPullThroughCache creates a registry acting as a pull through cache
func NewRegistryPullThroughCache(ctx context.Context, registry distribution.Namespace, driver driver.StorageDriver, config configuration.Proxy) (distribution.Namespace, error) {
	var remoteURLAll	[]url.URL
	var urlPrefixAll []string
	var remoteURL url.URL
	remoteURLStrAll := strings.Split(config.RemoteURL, ",")
	for _, urlStr := range remoteURLStrAll {
		t := strings.Split(urlStr,"#")
		parsedURL, err := url.Parse(t[0])
		if err != nil {
			return nil, err
		}
		remoteURLAll = append(remoteURLAll, *parsedURL)
		urlPrefixAll = append(urlPrefixAll, t[1])
	}

	remoteURL = remoteURLAll[0]

	v := storage.NewVacuum(ctx, driver)
	s := scheduler.New(ctx, driver, "/scheduler-state.json")
	s.OnBlobExpire(func(ref reference.Reference) error {
		var r reference.Canonical
		var ok bool
		if r, ok = ref.(reference.Canonical); !ok {
			return fmt.Errorf("unexpected reference type : %T", ref)
		}

		repo, err := registry.Repository(ctx, r)
		if err != nil {
			return err
		}

		blobs := repo.Blobs(ctx)

		// Clear the repository reference and descriptor caches
		err = blobs.Delete(ctx, r.Digest())
		if err != nil {
			return err
		}

		err = v.RemoveBlob(r.Digest().String())
		if err != nil {
			return err
		}

		return nil
	})

	s.OnManifestExpire(func(ref reference.Reference) error {
		var r reference.Canonical
		var ok bool
		if r, ok = ref.(reference.Canonical); !ok {
			return fmt.Errorf("unexpected reference type : %T", ref)
		}

		repo, err := registry.Repository(ctx, r)
		if err != nil {
			return err
		}

		manifests, err := repo.Manifests(ctx)
		if err != nil {
			return err
		}
		err = manifests.Delete(ctx, r.Digest())
		if err != nil {
			return err
		}
		return nil
	})

	err := s.Start()
	if err != nil {
		return nil, err
	}

	cs, err := configureAuth(config.Username, config.Password, remoteURL.String())
	if err != nil {
		return nil, err
	}

	return &proxyingRegistry{
		embedded:  registry,
		scheduler: s,
		remoteURL: remoteURL,
		remoteURLAll: remoteURLAll,
		urlPrefixAll: urlPrefixAll,
		authChallenger: &remoteAuthChallenger{
			remoteURL: remoteURL,
			remoteURLAll: remoteURLAll,
			cm:        challenge.NewSimpleManager(),
			cs:        cs,
		},
	}, nil
}

func (pr *proxyingRegistry) Scope() distribution.Scope {
	return distribution.GlobalScope
}

func (pr *proxyingRegistry) Repositories(ctx context.Context, repos []string, last string) (n int, err error) {
	return pr.embedded.Repositories(ctx, repos, last)
}

func (pr *proxyingRegistry) Repository(ctx context.Context, name reference.Named) (distribution.Repository, error) {
	c := pr.authChallenger

	tkopts := auth.TokenHandlerOptions{
		Transport:   http.DefaultTransport,
		Credentials: c.credentialStore(),
		Scopes: []auth.Scope{
			auth.RepositoryScope{
				Repository: name.Name(),
				Actions:    []string{"pull"},
			},
		},
		Logger: dcontext.GetLogger(ctx),
	}

	tr := transport.NewTransport(http.DefaultTransport,
		auth.NewAuthorizer(c.challengeManager(),
			auth.NewTokenHandlerWithOptions(tkopts)))

	localRepo, err := pr.embedded.Repository(ctx, name)
	if err != nil {
		return nil, err
	}
	localManifests, err := localRepo.Manifests(ctx, storage.SkipLayerVerification())
	if err != nil {
		return nil, err
	}

	var remoteStoreAll []distribution.BlobService
	var remoteManifestsAll []distribution.ManifestService
	var remoteTagsAll  []distribution.TagService

	for _ , remoteURL := range pr.remoteURLAll {
		remoteRepo, err := client.NewRepository(name, remoteURL.String(), tr)
		if err != nil {
			return nil, err
		}
		remoteManifests, err := remoteRepo.Manifests(ctx)
		if err != nil {
			return nil, err
		}
		remoteStoreAll = append(remoteStoreAll, remoteRepo.Blobs(ctx))
		remoteManifestsAll = append(remoteManifestsAll, remoteManifests)
		remoteTagsAll= append(remoteTagsAll, remoteRepo.Tags(ctx))
	}

	return &proxiedRepository{
		blobStore: &proxyBlobStore{
			localStore:     localRepo.Blobs(ctx),
			remoteStore:    nil,
			remoteStoreAll: remoteStoreAll,
			scheduler:      pr.scheduler,
			repositoryName: name,
			authChallenger: pr.authChallenger,
			urlPrefixAll: pr.urlPrefixAll,
		},
		manifests: &proxyManifestStore{
			repositoryName:  name,
			localManifests:  localManifests, // Options?
			remoteManifests: nil,
			remoteManifestsAll: remoteManifestsAll,
			ctx:             ctx,
			scheduler:       pr.scheduler,
			authChallenger:  pr.authChallenger,
			urlPrefixAll: pr.urlPrefixAll,
		},
		name: name,
		tags: &proxyTagService{
			localTags:      localRepo.Tags(ctx),
			remoteTags:     nil,
			remoteTagsAll:	remoteTagsAll,
			authChallenger: pr.authChallenger,
			urlPrefixAll: pr.urlPrefixAll,
		},
	}, nil
}

func (pr *proxyingRegistry) Blobs() distribution.BlobEnumerator {
	return pr.embedded.Blobs()
}

func (pr *proxyingRegistry) BlobStatter() distribution.BlobStatter {
	return pr.embedded.BlobStatter()
}

// authChallenger encapsulates a request to the upstream to establish credential challenges
type authChallenger interface {
	tryEstablishChallenges(context.Context, int) error
	challengeManager() challenge.Manager
	credentialStore() auth.CredentialStore
}

type remoteAuthChallenger struct {
	remoteURL url.URL
	remoteURLAll []url.URL
	sync.Mutex
	cm challenge.Manager
	cs auth.CredentialStore
}

func (r *remoteAuthChallenger) credentialStore() auth.CredentialStore {
	return r.cs
}

func (r *remoteAuthChallenger) challengeManager() challenge.Manager {
	return r.cm
}

// tryEstablishChallenges will attempt to get a challenge type for the upstream if none currently exist
func (r *remoteAuthChallenger) tryEstablishChallenges(ctx context.Context, idx int) error {
	r.Lock()
	defer r.Unlock()

	remoteURL := r.remoteURLAll[idx]
	remoteURL.Path = "/v2/"
	challenges, err := r.cm.GetChallenges(remoteURL)
	if err != nil {
		return err
	}

	if len(challenges) > 0 {
		return nil
	}

	// establish challenge type with upstream
	if err := ping(r.cm, remoteURL.String(), challengeHeader); err != nil {
		return err
	}

	dcontext.GetLogger(ctx).Infof("Challenge established with upstream : %s %s", remoteURL, r.cm)
	return nil
}

// proxiedRepository uses proxying blob and manifest services to serve content
// locally, or pulling it through from a remote and caching it locally if it doesn't
// already exist
type proxiedRepository struct {
	blobStore distribution.BlobStore
	manifests distribution.ManifestService
	name      reference.Named
	tags      distribution.TagService
}

func (pr *proxiedRepository) Manifests(ctx context.Context, options ...distribution.ManifestServiceOption) (distribution.ManifestService, error) {
	return pr.manifests, nil
}

func (pr *proxiedRepository) Blobs(ctx context.Context) distribution.BlobStore {
	return pr.blobStore
}

func (pr *proxiedRepository) Named() reference.Named {
	return pr.name
}

func (pr *proxiedRepository) Tags(ctx context.Context) distribution.TagService {
	return pr.tags
}
