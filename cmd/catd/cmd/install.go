package cmd

import (
	"github.com/google/uuid"
	v1 "github.com/operator-framework/operator-controller/api/v1"
	"github.com/perdasilva/operator-framework-commons/cmd/utils"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
)

func newInstallCmd(ac *utils.ActionConfig) (*cobra.Command, error) {
	var newInstallCmd = &cobra.Command{
		Use:   "install",
		Short: "Install catalog content",
		Long:  `Install catalog content`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			packageName := args[0]
			ext := utils.BuildClusterExtension(packageName)
			ext.Spec.Namespace = "test"
			ext.Spec.ServiceAccount.Name = "test-installer"
			ext.UID = types.UID(uuid.NewString())

			kernel := getKernel(ac)

			err := kernel.Sync(cmd.Context(), ext)
			if err != nil {
				return err
			}

			cond := meta.FindStatusCondition(ext.Status.Conditions, v1.TypeInstalled)
			if cond == nil {
				logger.Fatalf("Could not determine installation status: 'Installed' condition not found")
			}
			if cond.Status == "False" {
				logger.Errorf("Failed to install extension: %s", cond.Reason)
				return nil
			}
			logger.Infof(cond.Message)
			return nil
		},
	}
	return newInstallCmd, nil
}
