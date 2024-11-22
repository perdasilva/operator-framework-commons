package cmd

import (
	"fmt"
	catalogd "github.com/operator-framework/catalogd/api/v1"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
	"github.com/perdasilva/operator-framework-commons/cmd/utils"
	"github.com/spf13/cobra"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	outputFormatTypeYaml = "yaml"
	outputFormatTypeJson = "json"
)

func newGetCmd(ac *utils.ActionConfig) (*cobra.Command, error) {
	var getCmd = &cobra.Command{
		Use:   "get <catalogID>",
		Short: "Get the content of a catalog",
		Long:  `Get the content of a catalog.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			catalogID := args[0]
			format, err := cmd.Flags().GetString("output")
			if err != nil {
				return err
			}
			if format != outputFormatTypeJson && format != outputFormatTypeYaml {
				return fmt.Errorf("--format must be json or yaml")
			}

			catd := GetCatalogCache(ac)
			objKey := client.ObjectKey{
				Name: catalogID,
			}
			storedCatalog := &catalogd.ClusterCatalog{}
			err = catd.Get(cmd.Context(), objKey, storedCatalog)
			if err != nil {
				return err
			}
			catalogFS, err := catd.GetContentFS(cmd.Context(), storedCatalog)
			if err != nil {
				return err
			}

			err = declcfg.WalkFS(catalogFS, func(path string, cfg *declcfg.DeclarativeConfig, err error) error {
				if err != nil {
					return err
				}
				switch format {
				case outputFormatTypeYaml:
					return declcfg.WriteYAML(*cfg, os.Stdout)
				case outputFormatTypeJson:
					return declcfg.WriteJSON(*cfg, os.Stdout)
				default:
					return fmt.Errorf("unknown format type '%s'", format)
				}
			})
			if err != nil {
				return fmt.Errorf("failed to walk catalog FS: %w", err)
			}
			return nil
		},
	}
	getCmd.Flags().StringP("output", "o", outputFormatTypeJson, "Output format. Can either be \"json\", or \"yaml\".")
	return getCmd, nil
}
