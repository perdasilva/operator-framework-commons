package cmd

import (
	"github.com/olekukonko/tablewriter"
	"github.com/perdasilva/operator-framework-commons/cmd/utils"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

func newResolveCmd(ac *utils.ActionConfig) (*cobra.Command, error) {
	var resolveCmd = &cobra.Command{
		Use:   "resolve",
		Short: "Resolve catalog content",
		Long:  `Resolve catalog content`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			packageName := args[0]
			ext := utils.BuildClusterExtension(packageName)
			catd := GetCatalogCache(ac)
			res := GetResolver(catd)
			bundle, version, _, err := res.Resolve(cmd.Context(), ext, nil)
			if err != nil {
				return err
			}
			idx := strings.Index(bundle.Name, ".")
			name := bundle.Name
			if idx >= 0 {
				name = bundle.Name[:idx]
			}
			table := tablewriter.NewWriter(os.Stdout)
			table.SetHeader([]string{"NAME", "VERSION"})
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
			table.Append([]string{name, version.String()})
			table.Render()
			return nil
		},
	}
	return resolveCmd, nil
}
