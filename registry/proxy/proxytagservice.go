package proxy

import (
	"context"

	"github.com/docker/distribution"
)

// proxyTagService supports local and remote lookup of tags.
type proxyTagService struct {
	localTags      distribution.TagService
	remoteTags     distribution.TagService
	remoteTagsAll  []distribution.TagService
	authChallenger authChallenger
	urlPrefixAll	[]string
}

var _ distribution.TagService = proxyTagService{}

// Get attempts to get the most recent digest for the tag by checking the remote
// tag service first and then caching it locally.  If the remote is unavailable
// the local association is returned
func (pt proxyTagService) Get(ctx context.Context, tag string) (distribution.Descriptor, error) {

	var desc distribution.Descriptor
	var err error = nil
	for _, i := range MatchProjectPrefix(ctx, pt.urlPrefixAll) {
		if err = pt.authChallenger.tryEstablishChallenges(ctx, i); err != nil {
			return distribution.Descriptor{}, err
		}
		remoteTags := pt.remoteTagsAll[i]
		desc, err = remoteTags.Get(ctx, tag)
		if err == nil {
			break
		}
	}

	if err != nil {
		return desc, err
	}

	/*
	desc, err := pt.localTags.Get(ctx, tag)
	if err != nil {
		return distribution.Descriptor{}, err
	}
	*/
	return distribution.Descriptor{}, err
}

func (pt proxyTagService) Tag(ctx context.Context, tag string, desc distribution.Descriptor) error {
	return distribution.ErrUnsupported
}

func (pt proxyTagService) Untag(ctx context.Context, tag string) error {
	/*
	err := pt.localTags.Untag(ctx, tag)
	if err != nil {
		return err
	}
		 */
	return nil
}

func (pt proxyTagService) All(ctx context.Context) ([]string, error) {
	var tags []string = nil
	var err error = nil
	for _, i := range MatchProjectPrefix(ctx, pt.urlPrefixAll) {
		err := pt.authChallenger.tryEstablishChallenges(ctx, i)
		if err != nil {
			return nil, err
		}
		remoteTags := pt.remoteTagsAll[i]
		tags, err = remoteTags.All(ctx)
		if err == nil {
			break
		}
	}
	return tags, err
}

func (pt proxyTagService) Lookup(ctx context.Context, digest distribution.Descriptor) ([]string, error) {
	return []string{}, distribution.ErrUnsupported
}
