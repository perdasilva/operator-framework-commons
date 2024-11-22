package cmd

import (
	"github.com/olekukonko/tablewriter"
	catalogd "github.com/operator-framework/catalogd/api/v1"
	"github.com/perdasilva/operator-framework-commons/cmd/utils"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

func newListCmd(ac *utils.ActionConfig) (*cobra.Command, error) {
	return &cobra.Command{
		Use:   "list",
		Short: "List catalogs",
		Long:  `List catalogs`,
		RunE: func(cmd *cobra.Command, args []string) error {
			catd := GetCatalogCache(ac)

			catalogList := &catalogd.ClusterCatalogList{}
			err := catd.List(cmd.Context(), catalogList)
			if err != nil {
				return err
			}

			var data [][]string
			for _, catalog := range catalogList.Items {
				ref, err := utils.GetSourceUrlFromClusterCatalog(&catalog)
				if err != nil {
					panic(err)
				}
				lastUnpacked := "-"
				if catalog.Status.LastUnpacked != nil {
					lastUnpacked = catalog.Status.LastUnpacked.String()
				}
				icon := getIcon(&catalog)
				digest := getDigest(&catalog)
				data = append(data, []string{icon, catalog.Name, ref, digest, lastUnpacked})
			}

			table := tablewriter.NewWriter(os.Stdout)
			table.SetHeader([]string{"", "ID", "SOURCE", "DIGEST", "LAST UNPACKED"})
			table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
			table.SetAlignment(tablewriter.ALIGN_LEFT)
			table.SetBorder(false)
			table.SetCenterSeparator("")
			table.SetRowSeparator("")
			table.SetColumnSeparator("")
			table.SetTablePadding("\t")
			table.SetHeaderLine(false)
			table.SetAutoWrapText(false)
			table.SetAutoFormatHeaders(true)
			table.AppendBulk(data)
			table.Render()

			return nil
		},
	}, nil
}

func getDigest(catalog *catalogd.ClusterCatalog) string {
	if catalog.Status.ResolvedSource != nil {
		switch catalog.Status.ResolvedSource.Type {
		case catalogd.SourceTypeImage:
			return strings.Split(catalog.Status.ResolvedSource.Image.Ref, "@")[1]
		case catalogd.SourceTypeWeb:
			return catalog.Status.ResolvedSource.Web.Digest
		default:
			return "-"
		}
	}
	return "-"
}

func getIcon(catalog *catalogd.ClusterCatalog) string {
	switch catalog.Status.ResolvedSource.Type {
	case catalogd.SourceTypeImage:
		return "🐋"
	case catalogd.SourceTypeWeb:
		return "🌐"
	default:
		return "-"
	}
}
