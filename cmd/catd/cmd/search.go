package cmd

import (
	"fmt"
	"github.com/olekukonko/tablewriter"
	"github.com/perdasilva/operator-framework-commons/cmd/utils"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

func newSearchCmd(ac *utils.ActionConfig) (*cobra.Command, error) {
	var searchCmd = &cobra.Command{
		Use:   "search",
		Short: "Search catalog content",
		Long:  `Search catalog content`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			packageName := args[0]
			ext := utils.BuildClusterExtension(packageName)

			catd := GetCatalogCache(ac)
			res := GetResolver(catd)

			bundles, err := res.Search(cmd.Context(), ext)
			if err != nil {
				return err
			}
			if len(bundles) == 0 {
				return fmt.Errorf("no bundles found for package %s", packageName)
			}

			var data [][]string
			for _, b := range bundles {
				idx := strings.Index(b.Name, ".")
				name, version := b.Name, ""
				if idx >= 0 {
					name, version = b.Name[:idx], b.Name[idx+1:]
				}
				data = append(data, []string{b.CatalogID, name, version})
			}

			table := tablewriter.NewWriter(os.Stdout)
			table.SetHeader([]string{"CATALOGID", "NAME", "VERSION"})
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
	}
	return searchCmd, nil
}
