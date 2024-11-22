package cmd

import (
	v1 "github.com/operator-framework/catalogd/api/v1"
	"github.com/perdasilva/operator-framework-commons/cmd/utils"
	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func newRemoveCmd(ac *utils.ActionConfig) (*cobra.Command, error) {
	return &cobra.Command{
		Use:   "remove <catalogName>",
		Short: "Remove catalogs",
		Long:  `Remove catalogs`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			catalogName := args[0]
			catd := GetCatalogCache(ac)
			storedCatalog := &v1.ClusterCatalog{}
			objKey := client.ObjectKey{
				Name: catalogName,
			}
			if err := catd.Get(cmd.Context(), objKey, storedCatalog); err != nil {
				return err
			}
			if err := catd.DeleteCatalog(cmd.Context(), storedCatalog); err != nil {
				return err
			}
			logger.Infof("Removed catalog %s", catalogName)
			return nil
		},
	}, nil
}
