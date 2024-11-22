package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
	"github.com/operator-framework/operator-registry/alpha/property"
	"github.com/perdasilva/operator-framework-commons/pkg/catalogmetadata/filter"
	"io/fs"
)

// SourceTypeImage is the identifier for image-type bundle sources
const (
	SourceTypeImage SourceType = "image"
	SourceTypeHelm  SourceType = "helm"
)

type ImageSource struct {
	// Ref contains the reference to a container image containing Bundle contents.
	Ref string
}

type HelmSource struct {
	// contains the url to the helm chart
	Ref string
}

// BundleUnpacker unpacks bundle content, either synchronously or asynchronously and
// returns a Result, which conveys information about the progress of unpacking
// the bundle content.
//
// If a Source unpacks content asynchronously, it should register one or more
// watches with a controller to ensure that Bundles referencing this source
// can be reconciled as progress updates are available.
//
// For asynchronous Sources, multiple calls to Unpack should be made until the
// returned result includes state StateUnpacked.
//
// NOTE: A source is meant to be agnostic to specific bundle formats and
// specifications. A source should treat a bundle root directory as an opaque
// file tree and delegate bundle format concerns to bundle parsers.
type BundleUnpacker interface {
	Unpack(context.Context, *BundleSource) (*Result, error)
	Cleanup(context.Context, *BundleSource) error
}

// Result conveys progress information about unpacking bundle content.
type Result struct {
	// Bundle contains the full filesystem of a bundle's root directory.
	Bundle fs.FS

	// ResolvedSource is a reproducible view of a Bundle's Source.
	// When possible, source implementations should return a ResolvedSource
	// that pins the Source such that future fetches of the bundle content can
	// be guaranteed to fetch the exact same bundle content as the original
	// unpack.
	//
	// For example, resolved image sources should reference a container image
	// digest rather than an image tag, and git sources should reference a
	// commit hash rather than a branch or tag.
	ResolvedSource *BundleSource

	// State is the current state of unpacking the bundle content.
	State State

	// Message is contextual information about the progress of unpacking the
	// bundle content.
	Message string
}

type State string

const (
	// StateUnpacked conveys that the bundle has been successfully unpacked.
	StateUnpacked State = "Unpacked"
)

type SourceType string

type BundleSource struct {
	Name string
	// Type defines the kind of Bundle content being sourced.
	Type SourceType
	// Image is the bundle image that backs the content of this bundle.
	Image     *ImageSource
	HelmChart *HelmSource
}

func BundleSourceFromBundle(b *declcfg.Bundle) (*BundleSource, error) {
	if b == nil {
		return nil, errors.New("bundle is nil")
	}

	contentTypeProperties := filter.Filter(b.Properties, func(p property.Property) bool {
		return p.Type == "olm.content-type"
	})

	if len(contentTypeProperties) > 1 {
		return nil, errors.New("bundle contains more than one content type properties")
	}

	if len(contentTypeProperties) == 0 {
		contentType := ""
		if err := json.Unmarshal(contentTypeProperties[0].Value, &contentType); err != nil {
			return nil, fmt.Errorf("error unmarshalling package property: %w", err)
		}

		if contentType == "" {
			return nil, errors.New("bundle does not contain a content type property")
		}

		switch contentType {
		case "helm":
			return &BundleSource{
				Type: SourceTypeHelm,
				HelmChart: &HelmSource{
					Ref: b.Image,
				},
			}, nil
		default:
			return nil, fmt.Errorf("unknown content type %q", contentType)
		}
	}

	return &BundleSource{
		Type: SourceTypeImage,
		Image: &ImageSource{
			Ref: b.Image,
		},
	}, nil
}
