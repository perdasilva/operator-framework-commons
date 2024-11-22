package runtime

import (
	"context"
	"errors"
	"fmt"
	ocv1 "github.com/operator-framework/operator-controller/api/v1"
	"github.com/perdasilva/operator-framework-commons/pkg/bundleutil"
	"github.com/perdasilva/operator-framework-commons/pkg/resolve"
	"github.com/perdasilva/operator-framework-commons/pkg/runtime/labels"
	runtime "github.com/perdasilva/operator-framework-commons/pkg/runtime/source"
	"io/fs"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type InstalledBundleGetter interface {
	GetInstalledBundle(ctx context.Context, ext *ocv1.ClusterExtension) (*InstalledBundle, error)
}

type Applier interface {
	// Apply applies the content in the provided fs.FS using the configuration of the provided ClusterExtension.
	// It also takes in a map[string]string to be applied to all applied resources as labels and another
	// map[string]string used to create a unique identifier for a stored reference to the resources created.
	Apply(context.Context, fs.FS, *ocv1.ClusterExtension, map[string]string, map[string]string) ([]client.Object, string, error)
}

type Runtime struct {
	runtime.Unpacker
	Applier
	InstalledBundleGetter
}

type Kernel struct {
	Resolver resolve.Resolver
	Runtime  *Runtime
}

func (r *Kernel) Sync(ctx context.Context, ext *ocv1.ClusterExtension) error {
	l := log.FromContext(ctx)

	l.Info("getting installed bundle")
	installedBundle, err := r.Runtime.GetInstalledBundle(ctx, ext)
	if err != nil {
		setInstallStatus(ext, nil)
		setInstalledStatusConditionUnknown(ext, err.Error())
		setStatusProgressing(ext, errors.New("retrying to get installed bundle"))
		return err
	}

	// run resolution
	l.Info("resolving bundle")
	var bm *ocv1.BundleMetadata
	if installedBundle != nil {
		bm = &installedBundle.BundleMetadata
	}
	resolvedBundle, resolvedBundleVersion, resolvedDeprecation, err := r.Resolver.Resolve(ctx, ext, bm)
	if err != nil {
		// Note: We don't distinguish between resolution-specific errors and generic errors
		setStatusProgressing(ext, err)
		setInstalledStatusFromBundle(ext, installedBundle)
		ensureAllConditionsWithReason(ext, ocv1.ReasonFailed, err.Error())
		return err
	}

	// set deprecation status after _successful_ resolution
	// TODO:
	//  1. It seems like deprecation status should reflect the currently installed bundle, not the resolved
	//     bundle. So perhaps we should set package and channel deprecations directly after resolution, but
	//     defer setting the bundle deprecation until we successfully install the bundle.
	//  2. If resolution fails because it can't find a bundle, that doesn't mean we wouldn't be able to find
	//     a deprecation for the ClusterExtension's spec.packageName. Perhaps we should check for a non-nil
	//     resolvedDeprecation even if resolution returns an error. If present, we can still update some of
	//     our deprecation status.
	//       - Open question though: what if different catalogs have different opinions of what's deprecated.
	//         If we can't resolve a bundle, how do we know which catalog to trust for deprecation information?
	//         Perhaps if the package shows up in multiple catalogs and deprecations don't match, we can set
	//         the deprecation status to unknown? Or perhaps we somehow combine the deprecation information from
	//         all catalogs?
	SetDeprecationStatus(ext, resolvedBundle.Name, resolvedDeprecation)

	resolvedBundleMetadata := bundleutil.MetadataFor(resolvedBundle.Name, *resolvedBundleVersion)
	bundleSource := &runtime.BundleSource{
		Name: ext.GetName(),
		Type: runtime.SourceTypeImage,
		Image: &runtime.ImageSource{
			Ref: resolvedBundle.Image,
		},
	}

	l.Info("unpacking resolved bundle")
	unpackResult, err := r.Runtime.Unpack(ctx, bundleSource)
	if err != nil {
		// Wrap the error passed to this with the resolution information until we have successfully
		// installed since we intend for the progressing condition to replace the resolved condition
		// and will be removing the .status.resolution field from the ClusterExtension status API
		setStatusProgressing(ext, wrapErrorWithResolutionInfo(resolvedBundleMetadata, err))
		setInstalledStatusFromBundle(ext, installedBundle)
		return err
	}

	if unpackResult.State != runtime.StateUnpacked {
		panic(fmt.Sprintf("unexpected unpack state %q", unpackResult.State))
	}

	objLbls := map[string]string{
		labels.OwnerKindKey: ocv1.ClusterExtensionKind,
		labels.OwnerNameKey: ext.GetName(),
	}

	storeLbls := map[string]string{
		labels.BundleNameKey:      resolvedBundle.Name,
		labels.PackageNameKey:     resolvedBundle.Package,
		labels.BundleVersionKey:   resolvedBundleVersion.String(),
		labels.BundleReferenceKey: resolvedBundle.Image,
	}

	l.Info("applying bundle contents")
	// NOTE: We need to be cautious of eating errors here.
	// We should always return any error that occurs during an
	// attempt to apply content to the cluster. Only when there is
	// a verifiable reason to eat the error (i.e it is recoverable)
	// should an exception be made.
	// The following kinds of errors should be returned up the stack
	// to ensure exponential backoff can occur:
	//   - Permission errors (it is not possible to watch changes to permissions.
	//     The only way to eventually recover from permission errors is to keep retrying).
	_, _, err = r.Runtime.Apply(ctx, unpackResult.Bundle, ext, objLbls, storeLbls)
	if err != nil {
		setStatusProgressing(ext, wrapErrorWithResolutionInfo(resolvedBundleMetadata, err))
		// Now that we're actually trying to install, use the error
		setInstalledStatusFromBundle(ext, installedBundle)
		return err
	}

	newInstalledBundle := &InstalledBundle{
		BundleMetadata: resolvedBundleMetadata,
		Image:          resolvedBundle.Image,
	}
	// Successful install
	setInstalledStatusFromBundle(ext, newInstalledBundle)

	// If we made it here, we have successfully reconciled the ClusterExtension
	// and have reached the desired state. Since the Progressing status should reflect
	// our progress towards the desired state, we also set it when we have reached
	// the desired state by providing a nil error value.
	setStatusProgressing(ext, nil)
	return nil
}
