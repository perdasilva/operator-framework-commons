package cache

import (
	"context"
	"errors"
	"fmt"
	catalogd "github.com/operator-framework/catalogd/api/v1"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
	"github.com/perdasilva/operator-framework-commons/pkg/catalogmetadata/filter"
	"github.com/perdasilva/operator-framework-commons/pkg/client"
	"github.com/perdasilva/operator-framework-commons/pkg/source"
	"github.com/sirupsen/logrus"
	"io/fs"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/ptr"
	"os"
	ctrlrt "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"slices"
	"time"
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	// Register your custom resource in the scheme
	_ = catalogd.AddToScheme(scheme)
}

type CatalogCache struct {
	*client.Client
	source.Unpacker
	Logger *logrus.Logger
}

func (r *CatalogCache) GetPackage(ctx context.Context, catalog *catalogd.ClusterCatalog, pkgName string) (*declcfg.DeclarativeConfig, error) {

	catalogFsys, err := r.Unpacker.GetContentFS(ctx, catalog)
	if err != nil {
		return nil, err
	}

	var packages []declcfg.Package
	var bundles []declcfg.Bundle
	var channels []declcfg.Channel
	var deprecations []declcfg.Deprecation
	var others []declcfg.Meta
	err = declcfg.WalkFS(catalogFsys, func(path string, cfg *declcfg.DeclarativeConfig, err error) error {
		if err != nil {
			return err
		}
		packages = append(packages, filter.Filter(cfg.Packages, func(pkg declcfg.Package) bool {
			return pkg.Name == pkgName
		})...)
		bundles = append(bundles, filter.Filter(cfg.Bundles, func(bundle declcfg.Bundle) bool {
			return bundle.Package == pkgName
		})...)
		channels = append(channels, filter.Filter(cfg.Channels, func(channel declcfg.Channel) bool {
			return channel.Package == pkgName
		})...)
		deprecations = append(deprecations, filter.Filter(cfg.Deprecations, func(deprecation declcfg.Deprecation) bool {
			return deprecation.Package == pkgName
		})...)
		others = append(others, filter.Filter(cfg.Others, func(other declcfg.Meta) bool {
			return other.Package == pkgName
		})...)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &declcfg.DeclarativeConfig{
		Packages:     packages,
		Bundles:      bundles,
		Channels:     channels,
		Deprecations: deprecations,
		Others:       others,
	}, nil
}

func (r *CatalogCache) Sync(ctx context.Context, catalog *catalogd.ClusterCatalog) error {
	// Check if the catalog availability is set to disabled, if true then
	// unset base URL, delete it from the cache and set appropriate status
	if catalog.Spec.AvailabilityMode == catalogd.AvailabilityModeUnavailable {
		// Delete the catalog from local cache
		err := r.DeleteCatalog(ctx, catalog)
		if err != nil {
			return err
		}

		// Set status.conditions[type=Progressing] to False as we are done with
		// all that needs to be done with the catalog
		updateStatusProgressingUserSpecifiedUnavailable(&catalog.Status, catalog.GetGeneration())
		return nil
	}

	// TODO: The below algorithm to get the current state based on an in-memory
	//    storedCatalogs map is a hack that helps us keep the ClusterCatalog's
	//    status up-to-date. The fact that we need this setup is indicative of
	//    a larger problem with the design of one or both of the Unpacker and
	//    Storage interfaces and/or their interactions. We should fix this.
	expectedStatus, storedCatalog, err := r.getCurrentState(ctx, catalog)

	// If any of the following are true, we need to unpack the catalog:
	//   - we don't have a stored catalog in the map
	//   - we have a stored catalog, but the content doesn't exist on disk
	//   - we have a stored catalog, the content exists, but the expected status differs from the actual status
	//   - we have a stored catalog, the content exists, the status looks correct, but the catalog generation is different from the observed generation in the stored catalog
	//   - we have a stored catalog, the content exists, the status looks correct and reflects the catalog generation, but it is time to poll again
	needsUnpack := false
	switch {
	case storedCatalog == nil:
		needsUnpack = true
	case expectedStatus != nil && !equality.Semantic.DeepEqual(catalog.Status, *expectedStatus):
		needsUnpack = true
	case catalog.Generation != storedCatalog.GetGeneration():
		needsUnpack = true
	case r.needsPoll(storedCatalog.Status.LastUnpacked, catalog):
		needsUnpack = true
	}

	if !needsUnpack {
		// No need to update the status because we've already checked
		// that it is set correctly. Otherwise, we'd be unpacking again.
		return nil
	}

	unpackResult, err := r.Unpacker.Unpack(ctx, catalog)
	if err != nil {
		unpackErr := fmt.Errorf("source catalog content: %w", err)
		updateStatusProgressing(&catalog.Status, catalog.GetGeneration(), unpackErr)
		return unpackErr
	}

	switch unpackResult.State {
	case source.StateUnpacked:
		catalog.Status.LastUnpacked = ptr.To(metav1.NewTime(unpackResult.UnpackTime))
		catalog.Status.ResolvedSource = unpackResult.ResolvedSource
		updateStatusProgressing(&catalog.Status, catalog.GetGeneration(), nil)
	default:
		panic(fmt.Sprintf("unknown unpack state %q", unpackResult.State))
	}

	return nil
}

func hasContent(fsys fs.FS) bool {
	if fsys == nil {
		return false
	}

	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return false
	}
	return len(entries) > 0
}

func (r *CatalogCache) getCurrentState(ctx context.Context, catalog *catalogd.ClusterCatalog) (*catalogd.ClusterCatalogStatus, *catalogd.ClusterCatalog, error) {
	expectedStatus := catalog.Status.DeepCopy()

	// Set expected status based on what we see in the stored catalog
	clearUnknownConditions(expectedStatus)

	storedCatalog := &catalogd.ClusterCatalog{}
	err := r.Get(ctx, ctrlrt.ObjectKeyFromObject(catalog), storedCatalog)
	if err != nil {
		catalogFS, err := r.Unpacker.GetContentFS(ctx, storedCatalog)
		if err != nil && !os.IsNotExist(err) {
			return expectedStatus, nil, err
		} else if !hasContent(catalogFS) {
			return expectedStatus, nil, nil
		}
		updateStatusProgressing(expectedStatus, storedCatalog.GetGeneration(), nil)
	}

	return expectedStatus, storedCatalog, err
}

func clearUnknownConditions(status *catalogd.ClusterCatalogStatus) {
	knownTypes := sets.New[string](
		catalogd.TypeServing,
		catalogd.TypeProgressing,
	)
	status.Conditions = slices.DeleteFunc(status.Conditions, func(cond metav1.Condition) bool {
		return !knownTypes.Has(cond.Type)
	})
}

func updateStatusProgressing(status *catalogd.ClusterCatalogStatus, generation int64, err error) {
	progressingCond := metav1.Condition{
		Type:               catalogd.TypeProgressing,
		Status:             metav1.ConditionTrue,
		Reason:             catalogd.ReasonSucceeded,
		Message:            "Successfully unpacked and stored content from resolved source",
		ObservedGeneration: generation,
	}

	if err != nil {
		progressingCond.Status = metav1.ConditionTrue
		progressingCond.Reason = catalogd.ReasonRetrying
		progressingCond.Message = err.Error()
	}

	if errors.Is(err, reconcile.TerminalError(nil)) {
		progressingCond.Status = metav1.ConditionFalse
		progressingCond.Reason = catalogd.ReasonBlocked
	}

	meta.SetStatusCondition(&status.Conditions, progressingCond)
}

func updateStatusProgressingUserSpecifiedUnavailable(status *catalogd.ClusterCatalogStatus, generation int64) {
	// Set Progressing condition to True with reason Succeeded
	// since we have successfully progressed to the unavailable
	// availability mode and are ready to progress to any future
	// desired state.
	progressingCond := metav1.Condition{
		Type:               catalogd.TypeProgressing,
		Status:             metav1.ConditionTrue,
		Reason:             catalogd.ReasonSucceeded,
		Message:            "Catalog availability mode is set to Unavailable",
		ObservedGeneration: generation,
	}

	// Set Serving condition to False with reason UserSpecifiedUnavailable
	// so that users of this condition are aware that this catalog is
	// intentionally not being served
	servingCond := metav1.Condition{
		Type:               catalogd.TypeServing,
		Status:             metav1.ConditionFalse,
		Reason:             catalogd.ReasonUserSpecifiedUnavailable,
		Message:            "Catalog availability mode is set to Unavailable",
		ObservedGeneration: generation,
	}

	meta.SetStatusCondition(&status.Conditions, progressingCond)
	meta.SetStatusCondition(&status.Conditions, servingCond)
}

func (r *CatalogCache) needsPoll(lastSuccessfulPoll *metav1.Time, catalog *catalogd.ClusterCatalog) bool {
	if lastSuccessfulPoll == nil {
		return false
	}

	var pollInterval time.Duration
	switch catalog.Spec.Source.Type {
	case catalogd.SourceTypeWeb:
		if catalog.Spec.Source.Web.PollIntervalMinutes == nil {
			return false
		}
		pollInterval = time.Duration(*catalog.Spec.Source.Web.PollIntervalMinutes) * time.Minute
	case catalogd.SourceTypeImage:
		if catalog.Spec.Source.Image.PollIntervalMinutes == nil {
			return false
		}
		pollInterval = time.Duration(*catalog.Spec.Source.Image.PollIntervalMinutes) * time.Minute
	default:
		panic(fmt.Sprintf("unsupported source type: %s", catalog.Spec.Source.Type))
	}

	// Only poll if the next poll time is in the past.
	nextPoll := lastSuccessfulPoll.Add(pollInterval)
	return nextPoll.Before(time.Now())
}

func (r *CatalogCache) DeleteCatalog(ctx context.Context, catalog *catalogd.ClusterCatalog) error {
	if err := r.Unpacker.Cleanup(ctx, catalog); err != nil {
		return err
	}
	return r.Delete(ctx, catalog)
}
