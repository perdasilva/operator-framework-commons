package unpacker

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/perdasilva/operator-framework-commons/pkg/runtime/source"
	"io"
	"os"
	"path/filepath"
)

type TarGZ struct{}

func (i *TarGZ) Unpack(ctx context.Context, src string, dest string, opts *source.Options) error {
	inFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source: %w", err)
	}

	// Open a gzip reader
	gzReader, err := gzip.NewReader(inFile)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzReader.Close()

	err = os.MkdirAll(dest, 0700)
	if err != nil {
		return fmt.Errorf("error creating temporary directory: %w", err)
	}

	// Open a tar reader
	tarReader := tar.NewReader(gzReader)

	// Extract tar contents
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return fmt.Errorf("error reading tar header: %w", err)
		}

		// Strip the top-level directory from the path
		relativePath := header.Name

		// Construct the target file path
		targetPath := filepath.Join(dest, relativePath)

		switch header.Typeflag {
		case tar.TypeDir:
			// Create directory
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("error creating directory: %w", err)
			}
		case tar.TypeReg:
			// Ensure the directory for the file exists
			if err := os.MkdirAll(filepath.Dir(targetPath), os.FileMode(0700)); err != nil {
				return fmt.Errorf("error creating directory: %w", err)
			}

			// Create a file
			outFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("error creating file: %w", err)
			}
			if _, err := io.Copy(outFile, tarReader); err != nil {
				outFile.Close()
				return fmt.Errorf("error creating file: %w", err)
			}
			outFile.Close()
		}
	}
	return nil
}
