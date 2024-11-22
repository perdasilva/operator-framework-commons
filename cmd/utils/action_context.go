package utils

import (
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
)

type ActionConfig struct {
	BaseConfigPath  string
	Logger          *logrus.Logger
	SystemNamespace string
}

func GetConfigDirPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".catd"), nil
}
