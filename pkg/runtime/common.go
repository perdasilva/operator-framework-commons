package runtime

import (
	"errors"
	"fmt"
	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
	"github.com/perdasilva/operator-framework-commons/pkg/runtime/conditionsets"
	"k8s.io/apimachinery/pkg/util/sets"
	"strings"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	ocv1 "github.com/operator-framework/operator-controller/api/v1"
)

type InstalledBundle struct {
	ocv1.BundleMetadata
	Image string
}

func wrapErrorWithResolutionInfo(resolved ocv1.BundleMetadata, err error) error {
	return fmt.Errorf("%w for resolved bundle %q with version %q", err, resolved.Name, resolved.Version)
}

// SetDeprecationStatus will set the appropriate deprecation statuses for a ClusterExtension
// based on the provided bundle
func SetDeprecationStatus(ext *ocv1.ClusterExtension, bundleName string, deprecation *declcfg.Deprecation) {
	deprecations := map[string][]declcfg.DeprecationEntry{}
	channelSet := sets.New[string]()
	if ext.Spec.Source.Catalog != nil {
		for _, channel := range ext.Spec.Source.Catalog.Channels {
			channelSet.Insert(channel)
		}
	}
	if deprecation != nil {
		for _, entry := range deprecation.Entries {
			switch entry.Reference.Schema {
			case declcfg.SchemaPackage:
				deprecations[ocv1.TypePackageDeprecated] = []declcfg.DeprecationEntry{entry}
			case declcfg.SchemaChannel:
				if channelSet.Has(entry.Reference.Name) {
					deprecations[ocv1.TypeChannelDeprecated] = append(deprecations[ocv1.TypeChannelDeprecated], entry)
				}
			case declcfg.SchemaBundle:
				if bundleName != entry.Reference.Name {
					continue
				}
				deprecations[ocv1.TypeBundleDeprecated] = []declcfg.DeprecationEntry{entry}
			}
		}
	}

	// first get ordered deprecation messages that we'll join in the Deprecated condition message
	var deprecationMessages []string
	for _, conditionType := range []string{
		ocv1.TypePackageDeprecated,
		ocv1.TypeChannelDeprecated,
		ocv1.TypeBundleDeprecated,
	} {
		if entries, ok := deprecations[conditionType]; ok {
			for _, entry := range entries {
				deprecationMessages = append(deprecationMessages, entry.Message)
			}
		}
	}

	// next, set the Deprecated condition
	status, reason, message := metav1.ConditionFalse, ocv1.ReasonDeprecated, ""
	if len(deprecationMessages) > 0 {
		status, reason, message = metav1.ConditionTrue, ocv1.ReasonDeprecated, strings.Join(deprecationMessages, ";")
	}
	apimeta.SetStatusCondition(&ext.Status.Conditions, metav1.Condition{
		Type:               ocv1.TypeDeprecated,
		Reason:             reason,
		Status:             status,
		Message:            message,
		ObservedGeneration: ext.Generation,
	})

	// finally, set the individual deprecation conditions for package, channel, and bundle
	for _, conditionType := range []string{
		ocv1.TypePackageDeprecated,
		ocv1.TypeChannelDeprecated,
		ocv1.TypeBundleDeprecated,
	} {
		entries, ok := deprecations[conditionType]
		status, reason, message := metav1.ConditionFalse, ocv1.ReasonDeprecated, ""
		if ok {
			status, reason = metav1.ConditionTrue, ocv1.ReasonDeprecated
			for _, entry := range entries {
				message = fmt.Sprintf("%s\n%s", message, entry.Message)
			}
		}
		apimeta.SetStatusCondition(&ext.Status.Conditions, metav1.Condition{
			Type:               conditionType,
			Reason:             reason,
			Status:             status,
			Message:            message,
			ObservedGeneration: ext.Generation,
		})
	}
}

// ensureAllConditionsWithReason checks that all defined condition types exist in the given ClusterExtension,
// and assigns a specified reason and custom message to any missing condition.
func ensureAllConditionsWithReason(ext *ocv1.ClusterExtension, reason v1alpha1.ConditionReason, message string) {
	for _, condType := range conditionsets.ConditionTypes {
		cond := apimeta.FindStatusCondition(ext.Status.Conditions, condType)
		if cond == nil {
			// Create a new condition with a valid reason and add it
			newCond := metav1.Condition{
				Type:               condType,
				Status:             metav1.ConditionFalse,
				Reason:             string(reason),
				Message:            message,
				ObservedGeneration: ext.GetGeneration(),
				LastTransitionTime: metav1.NewTime(time.Now()),
			}
			ext.Status.Conditions = append(ext.Status.Conditions, newCond)
		}
	}
}

// setInstalledStatusFromBundle sets the installed status based on the given installedBundle.
func setInstalledStatusFromBundle(ext *ocv1.ClusterExtension, installedBundle *InstalledBundle) {
	// Nothing is installed
	if installedBundle == nil {
		setInstallStatus(ext, nil)
		setInstalledStatusConditionFailed(ext, "No bundle installed")
		return
	}
	// Something is installed
	installStatus := &ocv1.ClusterExtensionInstallStatus{
		Bundle: installedBundle.BundleMetadata,
	}
	setInstallStatus(ext, installStatus)
	setInstalledStatusConditionSuccess(ext, fmt.Sprintf("Installed bundle %s successfully", installedBundle.Image))
}

// setInstalledStatusConditionSuccess sets the installed status condition to success.
func setInstalledStatusConditionSuccess(ext *ocv1.ClusterExtension, message string) {
	apimeta.SetStatusCondition(&ext.Status.Conditions, metav1.Condition{
		Type:               ocv1.TypeInstalled,
		Status:             metav1.ConditionTrue,
		Reason:             ocv1.ReasonSucceeded,
		Message:            message,
		ObservedGeneration: ext.GetGeneration(),
	})
}

// setInstalledStatusConditionFailed sets the installed status condition to failed.
func setInstalledStatusConditionFailed(ext *ocv1.ClusterExtension, message string) {
	apimeta.SetStatusCondition(&ext.Status.Conditions, metav1.Condition{
		Type:               ocv1.TypeInstalled,
		Status:             metav1.ConditionFalse,
		Reason:             ocv1.ReasonFailed,
		Message:            message,
		ObservedGeneration: ext.GetGeneration(),
	})
}

// setInstalledStatusConditionUnknown sets the installed status condition to unknown.
func setInstalledStatusConditionUnknown(ext *ocv1.ClusterExtension, message string) {
	apimeta.SetStatusCondition(&ext.Status.Conditions, metav1.Condition{
		Type:               ocv1.TypeInstalled,
		Status:             metav1.ConditionUnknown,
		Reason:             ocv1.ReasonFailed,
		Message:            message,
		ObservedGeneration: ext.GetGeneration(),
	})
}

func setInstallStatus(ext *ocv1.ClusterExtension, installStatus *ocv1.ClusterExtensionInstallStatus) {
	ext.Status.Install = installStatus
}

func setStatusProgressing(ext *ocv1.ClusterExtension, err error) {
	progressingCond := metav1.Condition{
		Type:               ocv1.TypeProgressing,
		Status:             metav1.ConditionTrue,
		Reason:             ocv1.ReasonSucceeded,
		Message:            "desired state reached",
		ObservedGeneration: ext.GetGeneration(),
	}

	if err != nil {
		progressingCond.Reason = ocv1.ReasonRetrying
		progressingCond.Message = err.Error()
	}

	if errors.Is(err, reconcile.TerminalError(nil)) {
		progressingCond.Status = metav1.ConditionFalse
		progressingCond.Reason = ocv1.ReasonBlocked
	}

	apimeta.SetStatusCondition(&ext.Status.Conditions, progressingCond)
}
