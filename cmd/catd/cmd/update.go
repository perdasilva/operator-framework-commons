package cmd

import (
	catalogd "github.com/operator-framework/catalogd/api/v1"
	"github.com/perdasilva/operator-framework-commons/cmd/utils"
	"github.com/spf13/cobra"
)

func newUpdateCmd(ac *utils.ActionConfig) (*cobra.Command, error) {
	return &cobra.Command{
		Use:   "update",
		Short: "Update sources",
		Long:  `Update sources`,
		RunE: func(cmd *cobra.Command, args []string) error {
			catd := GetCatalogCache(ac)
			catalogs := &catalogd.ClusterCatalogList{}
			err := catd.List(cmd.Context(), catalogs)
			if err != nil {
				return err
			}
			for _, catalog := range catalogs.Items {
				sourceUrl, err := utils.GetSourceUrlFromClusterCatalog(&catalog)
				if err != nil {
					// Should never happen since catalogs returned from ListCatalogSources should be well-formed
					logger.Warnf("Skipping %s due to error: %v\n", catalog.Name, err)
					continue
				}
				logger.Infof("Updating catalog: %s from %s\n", catalog.Name, sourceUrl)
				nextVersion := catalog.DeepCopy()
				// Increase generation version to force update
				nextVersion.Generation = catalog.Generation + 1
				if err := catd.Sync(cmd.Context(), nextVersion); err != nil {
					logger.Warnf("Skipping %s due to error: %v\n", catalog.Name, err)
					continue
				}
				if err := catd.Update(cmd.Context(), nextVersion); err != nil {
					logger.Warnf("Skipping %s due to error: %v\n", catalog.Name, err)
					continue
				}
				logger.Infof("Successfully updated catalog")
			}
			return nil
		},
	}, nil
}
