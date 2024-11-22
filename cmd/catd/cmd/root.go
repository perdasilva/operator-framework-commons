package cmd

import (
	"context"
	"errors"
	"github.com/containers/image/v5/types"
	"github.com/go-logr/logr"
	catalogd "github.com/operator-framework/catalogd/api/v1"
	actionclient "github.com/operator-framework/helm-operator-plugins/pkg/client"
	helmclient "github.com/operator-framework/helm-operator-plugins/pkg/client"
	ocv1 "github.com/operator-framework/operator-controller/api/v1"
	"github.com/perdasilva/operator-framework-commons/cmd/utils"
	"github.com/perdasilva/operator-framework-commons/pkg/cache"
	"github.com/perdasilva/operator-framework-commons/pkg/client"
	"github.com/perdasilva/operator-framework-commons/pkg/resolve"
	ofrt "github.com/perdasilva/operator-framework-commons/pkg/runtime"
	"github.com/perdasilva/operator-framework-commons/pkg/runtime/action"
	"github.com/perdasilva/operator-framework-commons/pkg/runtime/applier"
	"github.com/perdasilva/operator-framework-commons/pkg/runtime/labels"
	"github.com/perdasilva/operator-framework-commons/pkg/runtime/preflights/crdupgradesafety"
	ofrtsrc "github.com/perdasilva/operator-framework-commons/pkg/runtime/source"
	"github.com/perdasilva/operator-framework-commons/pkg/source"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"helm.sh/helm/v3/pkg/release"
	"helm.sh/helm/v3/pkg/storage/driver"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1client "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"os"
	"path/filepath"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlrt "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := ocv1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := catalogd.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		panic(err)
	}
}

var logger = logrus.New()

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "catd",
	Short: "Manage FBC catalogs",
	Long:  `Manage FBC catalogs hosted on OCI image registries or web-servers. Download them locally, keep them up-to-date, and navigate them.`,
}

func getKernel(ac *utils.ActionConfig) *ofrt.Kernel {
	catd := GetCatalogCache(ac)
	resolver := GetResolver(catd)
	rt := getRuntime(ac)
	return &ofrt.Kernel{
		Resolver: resolver,
		Runtime:  rt,
	}
}

func getInstalledBundleGetter(ac action.ActionClientGetter) ofrt.InstalledBundleGetter {
	return &DefaultInstalledBundleGetter{ActionClientGetter: ac}
}

func getRegistryV1Runtime(ac *utils.ActionConfig, clust cluster.Cluster) (*ofrt.Runtime, error) {
	unpackPath := filepath.Join(ac.BaseConfigPath, "bundle-unpack")
	unpacker := &ofrtsrc.ContainersImageRegistry{
		BaseCachePath: unpackPath,
		SourceContextFunc: func(_ logr.Logger) (*types.SystemContext, error) {
			return &types.SystemContext{}, nil
		},
	}

	actionClient, err := getActionClient(ac, clust)
	if err != nil {
		panic(err)
	}

	aeClient, err := apiextensionsv1client.NewForConfig(clust.GetConfig())
	if err != nil {
		panic(err)
	}

	preflights := []applier.Preflight{
		crdupgradesafety.NewPreflight(aeClient.CustomResourceDefinitions()),
	}

	applr := &applier.RegistryV1{
		ActionClientGetter: actionClient,
		Preflights:         preflights,
	}

	return &ofrt.Runtime{
		BundleUnpacker: unpacker,
		Applier:        applr,
	}, nil
}

func getHelmRuntime(ac *utils.ActionConfig, clust cluster.Cluster) (*ofrt.Runtime, error) {
	unpackPath := filepath.Join(ac.BaseConfigPath, "bundle-unpack")
	unpacker := &ofrtsrc.TarGZ{
		BaseCachePath: unpackPath,
	}

	actionClient, err := getActionClient(ac, clust)
	if err != nil {
		panic(err)
	}

	aeClient, err := apiextensionsv1client.NewForConfig(clust.GetConfig())
	if err != nil {
		panic(err)
	}

	preflights := []applier.Preflight{
		crdupgradesafety.NewPreflight(aeClient.CustomResourceDefinitions()),
	}

	applr := &applier.RegistryV1{
		ActionClientGetter: actionClient,
		Preflights:         preflights,
	}

	return &ofrt.Runtime{
		BundleUnpacker: unpacker,
		Applier:        applr,
	}, nil
}

func getRuntime(ac *utils.ActionConfig) *ofrt.Runtime {
	clust, err := getCluster()
	if err != nil {
		logger.Fatal(err)
	}
	unpackPath := filepath.Join(ac.BaseConfigPath, "bundle-unpack")
	unpacker := &ofrtsrc.ContainersImageRegistry{
		BaseCachePath: unpackPath,
		SourceContextFunc: func(_ logr.Logger) (*types.SystemContext, error) {
			return &types.SystemContext{}, nil
		},
	}

	actionClient, err := getActionClient(ac, clust)
	if err != nil {
		panic(err)
	}

	aeClient, err := apiextensionsv1client.NewForConfig(clust.GetConfig())
	if err != nil {
		panic(err)
	}

	preflights := []applier.Preflight{
		crdupgradesafety.NewPreflight(aeClient.CustomResourceDefinitions()),
	}

	applier := &applier.RegistryV1{
		ActionClientGetter: actionClient,
		Preflights:         preflights,
	}

	return &ofrt.Runtime{
		BundleUnpacker:        unpacker,
		Applier:               applier,
		InstalledBundleGetter: &DefaultInstalledBundleGetter{ActionClientGetter: actionClient},
	}
}

func GetCatalogCache(ac *utils.ActionConfig) *cache.CatalogCache {
	return &cache.CatalogCache{
		Client:   getCatalogdClient(ac),
		Unpacker: getUnpacker(ac),
		Logger:   ac.Logger,
	}
}

func getCluster() (cluster.Cluster, error) {
	cfg, err := ctrl.GetConfig()
	if err != nil {
		panic(err)
	}
	return cluster.New(cfg, func(options *cluster.Options) {
		options.Scheme = scheme
	})
}

func getActionClient(ac *utils.ActionConfig, cluster cluster.Cluster) (actionclient.ActionClientGetter, error) {
	coreClient, err := corev1client.NewForConfig(cluster.GetConfig())
	if err != nil {
		return nil, err
	}

	cfgGetter, err := helmclient.NewActionConfigGetter(cluster.GetConfig(), cluster.GetRESTMapper(),
		helmclient.StorageDriverMapper(action.ChunkedStorageDriverMapper(coreClient, cluster.GetAPIReader(), ac.SystemNamespace)),
		helmclient.ClientNamespaceMapper(func(obj ctrlrt.Object) (string, error) {
			ext := obj.(*ocv1.ClusterExtension)
			return ext.Spec.Namespace, nil
		}),
	)
	if err != nil {
		return nil, err
	}

	acg, err := action.NewWrappedActionClientGetter(cfgGetter,
		helmclient.WithFailureRollbacks(false),
	)
	return acg, nil
}

func getKubeconfig() ([]byte, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	kubeconfig := filepath.Join(home, ".kube", "config")
	configData, err := os.ReadFile(kubeconfig)
	if err != nil {
		return nil, err
	}
	return configData, nil
}

func getCatalogdClient(ac *utils.ActionConfig) *client.Client {
	scheme := runtime.NewScheme()
	_ = catalogd.AddToScheme(scheme)
	_ = ocv1.AddToScheme(scheme)
	return client.NewClient(ac.BaseConfigPath, scheme)
}

func getUnpacker(ac *utils.ActionConfig) source.Unpacker {
	unpackPath := filepath.Join(ac.BaseConfigPath, "unpack")
	return &source.MultiSourceUnpacker{
		Router: map[catalogd.SourceType]source.Unpacker{
			catalogd.SourceTypeWeb: &source.WebRegistry{
				BaseCachePath: unpackPath,
				Logger:        logger,
			},
			catalogd.SourceTypeImage: &source.ContainersImageRegistry{
				BaseCachePath: unpackPath,
				Logger:        logger,
				SourceContextFunc: func(_ logr.Logger) (*types.SystemContext, error) {
					return &types.SystemContext{}, nil
				},
			},
		},
	}
}

func GetResolver(catalogCache *cache.CatalogCache) *resolve.CatalogResolver {
	return &resolve.CatalogResolver{
		WalkCatalogsFunc: resolve.CatalogWalker(
			func(ctx context.Context, option ...ctrlrt.ListOption) ([]catalogd.ClusterCatalog, error) {
				catalogs := catalogd.ClusterCatalogList{}
				if err := catalogCache.List(ctx, &catalogs, option...); err != nil {
					return nil, err
				}
				return catalogs.Items, nil
			},
			catalogCache.GetPackage,
		),
		Validations: []resolve.ValidationFunc{
			resolve.NoDependencyValidation,
		},
	}
}

type DefaultInstalledBundleGetter struct {
	helmclient.ActionClientGetter
}

func (d *DefaultInstalledBundleGetter) GetInstalledBundle(ctx context.Context, ext *ocv1.ClusterExtension) (*ofrt.InstalledBundle, error) {
	cl, err := d.ActionClientFor(ctx, ext)
	if err != nil {
		return nil, err
	}

	relhis, err := cl.History(ext.GetName())
	if err != nil && !errors.Is(err, driver.ErrReleaseNotFound) {
		return nil, err
	}
	if len(relhis) == 0 {
		return nil, nil
	}

	// relhis[0].Info.Status is the status of the most recent install attempt.
	// But we need to look for the most-recent _Deployed_ release
	for _, rel := range relhis {
		if rel.Info != nil && rel.Info.Status == release.StatusDeployed {
			return &ofrt.InstalledBundle{
				BundleMetadata: ocv1.BundleMetadata{
					Name:    rel.Labels[labels.BundleNameKey],
					Version: rel.Labels[labels.BundleVersionKey],
				},
				Image: rel.Labels[labels.BundleReferenceKey],
			}, nil
		}
	}
	return nil, nil
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Create initial action context
	baseConfigPath, err := utils.GetConfigDirPath()
	if err != nil {
		panic(err)
	}
	ac := &utils.ActionConfig{
		BaseConfigPath:  baseConfigPath,
		Logger:          logger,
		SystemNamespace: "olmv1-system",
	}

	// Configure the logger
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logger.SetLevel(logrus.InfoLevel)

	// Add persistent flags
	rootCmd.PersistentFlags().String("log-level", "info", "Set the logging level (debug, info, warn, error)")
	rootCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		level, _ := cmd.Flags().GetString("log-level")
		parsedLevel, err := logrus.ParseLevel(level)
		if err != nil {
			ac.Logger.Warn("Invalid log level, defaulting to 'info'")
			parsedLevel = logrus.InfoLevel
		}
		ac.Logger.SetLevel(parsedLevel)
	}

	// Add sub-commands
	subCmdFactories := []func(ac *utils.ActionConfig) (*cobra.Command, error){
		newAddCmd,
		newRemoveCmd,
		newListCmd,
		newUpdateCmd,
		newGetCmd,
		newSearchCmd,
		newResolveCmd,
		newInstallCmd,
	}

	for _, newSubCmd := range subCmdFactories {
		subCmd, err := newSubCmd(ac)
		if err != nil {
			panic(err)
		}
		rootCmd.AddCommand(subCmd)
	}
}
