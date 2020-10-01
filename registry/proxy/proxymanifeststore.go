package proxy

import (
	"context"
	"time"
	"errors"

	"github.com/docker/distribution"
	//dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/proxy/scheduler"
	"github.com/opencontainers/go-digest"
)

// todo(richardscothern): from cache control header or config
const repositoryTTL = 24 * 7 * time.Hour

type proxyManifestStore struct {
	ctx             context.Context
	localManifests  distribution.ManifestService
	remoteManifests distribution.ManifestService
	remoteManifestsAll []distribution.ManifestService
	repositoryName  reference.Named
	scheduler       *scheduler.TTLExpirationScheduler
	authChallenger  authChallenger
	urlPrefixAll	[]string
}

var _ distribution.ManifestService = &proxyManifestStore{}

func (pms proxyManifestStore) Exists(ctx context.Context, dgst digest.Digest) (bool, error) {
	/*
	exists, err := pms.localManifests.Exists(ctx, dgst)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}
	*/

	var exists bool = false
	var err error

	for _, i := range MatchProjectPrefix(ctx, pms.urlPrefixAll) {
		if err = pms.authChallenger.tryEstablishChallenges(ctx, i); err != nil {
			return false, err
		}
		remoteManifests := pms.remoteManifestsAll[i]
		exists, err = remoteManifests.Exists(ctx, dgst)
		if exists == true && err == nil {
			break
		}
	}

	return exists, err
}

func (pms proxyManifestStore) Get(ctx context.Context, dgst digest.Digest, options ...distribution.ManifestServiceOption) (distribution.Manifest, error) {
	// At this point `dgst` was either specified explicitly, or returned by the
	// tagstore with the most recent association.
	//var fromRemote bool = true

	/*
	manifest, err := pms.localManifests.Get(ctx, dgst, options...)
	if err != nil {
		if err := pms.authChallenger.tryEstablishChallenges(ctx); err != nil {
			return nil, err
		}

		manifest, err = pms.remoteManifests.Get(ctx, dgst, options...)
		if err != nil {
			return nil, err
		}
		fromRemote = true
	}
	*/


	var manifest distribution.Manifest = nil
	var err error = nil

	for _, i := range MatchProjectPrefix(ctx, pms.urlPrefixAll) {
		if err := pms.authChallenger.tryEstablishChallenges(ctx, i); err != nil {
			return nil, err
		}
		remoteManifests := pms.remoteManifestsAll[i]
		manifest, err = remoteManifests.Get(ctx, dgst, options...)
		if err == nil {
			break
		}
	}

	if err != nil {
		return nil, err
	}

	if manifest == nil {
		return nil, errors.New("no project prefix matched")
	}

	_, payload, err := manifest.Payload()
	if err != nil {
		return nil, err
	}

	proxyMetrics.ManifestPush(uint64(len(payload)))

	/*
	if fromRemote {
		proxyMetrics.ManifestPull(uint64(len(payload)))

		_, err = pms.localManifests.Put(ctx, manifest)
		if err != nil {
			return nil, err
		}

		// Schedule the manifest blob for removal
		repoBlob, err := reference.WithDigest(pms.repositoryName, dgst)
		if err != nil {
			dcontext.GetLogger(ctx).Errorf("Error creating reference: %s", err)
			return nil, err
		}

		pms.scheduler.AddManifest(repoBlob, repositoryTTL)
		// Ensure the manifest blob is cleaned up
		//pms.scheduler.AddBlob(blobRef, repositoryTTL)
	}
	*/

	return manifest, err
}

func (pms proxyManifestStore) Put(ctx context.Context, manifest distribution.Manifest, options ...distribution.ManifestServiceOption) (digest.Digest, error) {
	var d digest.Digest
	return d, distribution.ErrUnsupported
}

func (pms proxyManifestStore) Delete(ctx context.Context, dgst digest.Digest) error {
	return distribution.ErrUnsupported
}
