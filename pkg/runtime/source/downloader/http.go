package downloader

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/perdasilva/operator-framework-commons/pkg/runtime/source"
	"github.com/perdasilva/operator-framework-commons/pkg/runtime/source/progress"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

const (
	TaskIDHTTPDownload   = "download"
	TaskIDHTTPDecompress = "decompress"
)

type HTTPDownloader struct {
	Client *http.Client
}

func (h *HTTPDownloader) Download(ctx context.Context, url string, dest string, opts source.Options) error {
	// Get content from webserver
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("error creating HTTP request: %w", err)
	}

	// Set the Accept-Encoding header to request gzipped content
	req.Header.Set("Accept-Encoding", "gzip")

	// Download content locally
	response, err := h.Client.Do(req)
	if err != nil {
		return fmt.Errorf("error making GET request: %w", err)
	}
	defer response.Body.Close()

	// Check if the response is successful
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", response.StatusCode)
	}

	destFilePath := filepath.Join(dest, "catalog.yaml")
	if err := os.MkdirAll(dest, 0777); err != nil {
		return fmt.Errorf("error creating destination directory: %w", err)
	}
	outFile, err := os.Create(destFilePath)
	if err != nil {
		return fmt.Errorf("error creating file for download: %w", err)
	}
	defer outFile.Close()

	var out io.ReadWriter
	if response.Header.Get("Content-Encoding") == "gzip" {
		out = &bytes.Buffer{}
	} else {
		out = outFile
	}

	progressEmitter := &progress.Emitter{
		ProgressChan: opts.ProgressChan,
	}

	progressEmitter.NewTask(TaskIDHTTPDownload).WithSize(response.ContentLength).Emit()
	if err := copyData(ctx, progressEmitter, response.Body, out); err != nil {
		return fmt.Errorf("error downloading data: %w", err)
	}
	progressEmitter.Done().Emit()

	if response.Header.Get("Content-Encoding") == "gzip" {
		// We don't know the final size of the decompressed file
		progressEmitter.TaskID = "Decompressing"
		progressEmitter.Size = -1

		// Get gzip reader
		reader, err := gzip.NewReader(out)
		if err != nil {
			return fmt.Errorf("error creating gzip reader: %w", err)
		}
		defer reader.Close()

		progressEmitter.NewTask(TaskIDHTTPDecompress).WithSize(-1).Emit()
		if err := copyData(ctx, progressEmitter, reader, outFile); err != nil {
			return fmt.Errorf("error decompressing data: %w", err)
		}
	}

	return nil
}

func copyData(ctx context.Context, progressEmitter *progress.Emitter, reader io.Reader, out io.Writer) error {
	var totalBytes int64 = 0
	buffer := make([]byte, 32*1024)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Read a chunk
			n, err := reader.Read(buffer)
			if n > 0 {
				// Write the chunk to the file
				if _, writeErr := out.Write(buffer[:n]); writeErr != nil {
					return fmt.Errorf("failed to write data: %v", writeErr)
				}
				progressEmitter.WithProgress(totalBytes, int64(n)).Progress().Emit()
			}

			// Check for end of file or error
			if err == io.EOF {
				progressEmitter.Done().Emit()
				return nil
			}
			if err != nil {
				return fmt.Errorf("error reading response body: %v", err)
			}
		}
	}
}
