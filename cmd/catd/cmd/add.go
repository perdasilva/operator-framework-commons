package cmd

import (
	"fmt"
	catalogd "github.com/operator-framework/catalogd/api/v1"
	"github.com/perdasilva/operator-framework-commons/cmd/utils"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func newAddCmd(ac *utils.ActionConfig) (*cobra.Command, error) {
	var addCmd = &cobra.Command{
		Use:   "add <catalogUrl>",
		Short: "Add an FBC catalog",
		Long: `Add an FBC catalog. Examples:

# Add an oci-based catalog
catd add oci://my-repository.io/my-image:tag

# Add a web-based catalog
catd add https://my-url.io/my-catalog
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			catalogUrl := args[0]
			catd := GetCatalogCache(ac)
			catalogID := utils.MakeCatalogID(catalogUrl)
			storedCatalog := &catalogd.ClusterCatalog{}
			objKey := client.ObjectKey{
				Name: catalogID,
			}
			err := catd.Get(cmd.Context(), objKey, storedCatalog)
			if err != nil && !errors.IsNotFound(err) {
				return fmt.Errorf("failed to retrieve catalog: %w", err)
			} else if errors.IsNotFound(err) {
				storedCatalog, err = utils.BuildClusterCatalogFromUrlString(catalogUrl)
				if err != nil {
					return fmt.Errorf("failed create catalog: %w", err)
				}
			}

			if err := catd.Sync(cmd.Context(), storedCatalog); err != nil {
				return fmt.Errorf("failed to add catalog: %w", err)
			}

			if err := catd.Update(cmd.Context(), storedCatalog); err != nil {
				return fmt.Errorf("failed to update catalog: %w", err)
			}

			return nil
		},
	}
	return addCmd, nil
}
