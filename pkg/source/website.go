package source

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	catalogdv1alpha1 "github.com/operator-framework/catalogd/api/v1"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
	"github.com/sirupsen/logrus"
	"io"
	"io/fs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strings"
	"time"
)

type WebRegistry struct {
	BaseCachePath string
	Logger        *logrus.Logger
}

func (w *WebRegistry) GetContentFS(ctx context.Context, catalog *catalogdv1alpha1.ClusterCatalog) (fs.FS, error) {
	if catalog == nil || catalog.Status.ResolvedSource == nil {
		return nil, fmt.Errorf("catalog has not been sync'd yet")
	}

	var ref string
	switch catalog.Status.ResolvedSource.Type {
	case catalogdv1alpha1.SourceTypeImage:
		ref = strings.Split(catalog.Status.ResolvedSource.Image.Ref, "@")[1]
	case catalogdv1alpha1.SourceTypeWeb:
		ref = catalog.Status.ResolvedSource.Web.Digest
	default:
		panic(fmt.Sprintf("Unsupported source type: %s", catalog.Status.ResolvedSource.Type))
	}

	dataPath := filepath.Join(w.BaseCachePath, catalog.Name, ref)
	if fstat, err := os.Stat(dataPath); err != nil {
		return nil, err
	} else if !fstat.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", dataPath)
	}

	return os.DirFS(dataPath), nil
}

func (w *WebRegistry) Unpack(ctx context.Context, catalog *catalogdv1alpha1.ClusterCatalog) (*Result, error) {
	// Check preconditions
	if catalog.Spec.Source.Type != catalogdv1alpha1.SourceTypeWeb {
		panic(fmt.Sprintf("programmer error: source type %q is unable to handle specified catalog source type %q", catalogdv1alpha1.SourceTypeWeb, catalog.Spec.Source.Type))
	}

	if catalog.Spec.Source.Web == nil {
		return nil, reconcile.TerminalError(fmt.Errorf("error parsing catalog, catalog %s has a nil image source", catalog.Name))
	}

	catalogUrl, err := url.JoinPath(catalog.Spec.Source.Web.URL, "catalog.yaml")
	if err != nil {
		return nil, reconcile.TerminalError(fmt.Errorf("invalid catalog URL %q: %w", catalog.Spec.Source.Web.URL, err))
	}
	if _, err := url.Parse(catalogUrl); err != nil {
		return nil, reconcile.TerminalError(fmt.Errorf("invalid catalog URL %q: %w", catalogUrl, err))
	}

	w.Logger.Infof("Downloading catalog...")
	tmpFilePath, err := downloadCatalog(catalog.Name, catalogUrl)
	if err != nil {
		return nil, fmt.Errorf("error downloading catalog: %w", err)
	}

	w.Logger.Infof("Validating content...")
	if err := validateContent(ctx, tmpFilePath); err != nil {
		return nil, fmt.Errorf("error validating catalog content: %w", err)
	}

	contentDigest, err := computeSHA256(tmpFilePath)
	if err != nil {
		return nil, fmt.Errorf("error computing contentDigest of catalog content: %w", err)
	}
	w.Logger.Infof("Content digest: %s", contentDigest)

	unpackDirPath := w.unpackPath(catalog.Name, contentDigest)
	if unpackStat, err := os.Stat(unpackDirPath); err == nil {
		w.Logger.Infof("No new updates to catalog")
		return &Result{
			FS: os.DirFS(unpackDirPath),
			ResolvedSource: &catalogdv1alpha1.ResolvedCatalogSource{
				Type: catalogdv1alpha1.SourceTypeWeb,
				Web: &catalogdv1alpha1.ResolvedWebSource{
					URL:    catalogUrl,
					Digest: contentDigest,
				},
			},
			State:   StateUnpacked,
			Message: fmt.Sprintf("unpacked %q successfully", contentDigest),

			// We truncate both the unpack time and last successful poll attempt
			// to the second because metav1.Time is serialized
			// as RFC 3339 which only has second-level precision. When we
			// use this result in a comparison with what we deserialized
			// from the Kubernetes API server, we need it to match.
			UnpackTime:                unpackStat.ModTime().Truncate(time.Second),
			LastSuccessfulPollAttempt: metav1.NewTime(time.Now().Truncate(time.Second)),
		}, nil
	}

	lastUnpacked, err := saveWebCatalog(tmpFilePath, unpackDirPath)
	if err != nil {
		if cleanupErr := w.Cleanup(ctx, catalog); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
		return nil, fmt.Errorf("error saving catalog: %w", err)
	}

	w.Logger.Infof("Cleaning up...")
	if err := w.deleteOtherCatalogs(catalog.Name, contentDigest); err != nil {
		return nil, fmt.Errorf("error deleting old catalogs: %w", err)
	}

	return &Result{
		FS: os.DirFS(unpackDirPath),
		ResolvedSource: &catalogdv1alpha1.ResolvedCatalogSource{
			Type: catalogdv1alpha1.SourceTypeWeb,
			Web: &catalogdv1alpha1.ResolvedWebSource{
				URL:    catalogUrl,
				Digest: contentDigest,
			},
		},
		State:   StateUnpacked,
		Message: fmt.Sprintf("unpacked %q successfully", contentDigest),

		// We truncate both the unpack time and last successful poll attempt
		// to the second because metav1.Time is serialized
		// as RFC 3339 which only has second-level precision. When we
		// use this result in a comparison with what we deserialized
		// from the Kubernetes API server, we need it to match.
		UnpackTime:                lastUnpacked.Truncate(time.Second),
		LastSuccessfulPollAttempt: metav1.NewTime(time.Now().Truncate(time.Second)),
	}, nil
}

func (w *WebRegistry) Cleanup(_ context.Context, catalog *catalogdv1alpha1.ClusterCatalog) error {
	if err := deleteRecursive(w.catalogPath(catalog.Name)); err != nil {
		return fmt.Errorf("error deleting catalog cache: %w", err)
	}
	return nil
}

func (w *WebRegistry) catalogPath(catalogName string) string {
	return filepath.Join(w.BaseCachePath, catalogName)
}

func (w *WebRegistry) unpackPath(catalogName string, digest string) string {
	return filepath.Join(w.catalogPath(catalogName), digest)
}

func (w *WebRegistry) deleteOtherCatalogs(catalogName string, digestToKeep string) error {
	catalogPath := w.catalogPath(catalogName)
	imgDirs, err := os.ReadDir(catalogPath)
	if err != nil {
		return fmt.Errorf("error reading image directories: %w", err)
	}
	for _, imgDir := range imgDirs {
		if imgDir.Name() == digestToKeep {
			continue
		}
		imgDirPath := filepath.Join(catalogPath, imgDir.Name())
		if err := deleteRecursive(imgDirPath); err != nil {
			return fmt.Errorf("error removing image directory: %w", err)
		}
	}
	return nil
}

func computeSHA256(filePath string) (string, error) {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	// Create a new SHA256 hash
	hash := sha256.New()

	// Copy the file's content into the hash
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("error hashing file: %w", err)
	}

	// Get the SHA256 checksum as a hexadecimal string
	return fmt.Sprintf("sha256:%x", hash.Sum(nil)), nil
}

func validateContent(ctx context.Context, filePath string) error {
	_, err := declcfg.LoadFS(ctx, os.DirFS(filepath.Dir(filePath)))
	if err != nil {
		return fmt.Errorf("error loading catalog content: %w", err)
	}
	return nil
}

func downloadCatalog(catalogName string, catalogUrl string) (string, error) {
	// Get content from webserver
	client := &http.Client{}
	req, err := http.NewRequest("GET", catalogUrl, nil)
	if err != nil {
		return "", fmt.Errorf("error creating HTTP request: %w", err)
	}

	// Set the Accept-Encoding header to request gzipped content
	req.Header.Set("Accept-Encoding", "gzip")

	// Download content locally
	response, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error making GET request: %w", err)
	}
	defer response.Body.Close()

	// Check if the server sent a gzip-compressed response
	var reader io.ReadCloser
	if response.Header.Get("Content-Encoding") == "gzip" {
		// Create a gzip reader to decompress the response body
		reader, err = gzip.NewReader(response.Body)
		if err != nil {
			return "", fmt.Errorf("error creating gzip reader: %w", err)
		}
		defer reader.Close()
	} else {
		// Use the response body directly if not compressed
		reader = response.Body
	}

	// Check if the response is successful
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", response.StatusCode)
	}

	content, err := io.ReadAll(reader)
	if err != nil {
		return "", fmt.Errorf("error reading response body: %w", err)
	}

	// Create tmp dir download file
	tmpDir := path.Join(os.TempDir(), "catalogs", catalogName)
	if err := os.MkdirAll(tmpDir, 0700); err != nil {
		return "", fmt.Errorf("error creating temp directory: %w", err)
	}

	// Create tmp download file
	tmpFilePath := path.Join(tmpDir, fmt.Sprintf("catalog-%s.yaml", catalogName))
	tmpFile, err := os.Create(tmpFilePath)
	defer tmpFile.Close()
	if err != nil {
		return "", fmt.Errorf("error creating temp file: %w", err)
	}

	// Download response to tmp file
	_, err = io.Copy(tmpFile, strings.NewReader(string(content)))
	if err != nil {
		return "", fmt.Errorf("error downloading catalog content: %w", err)
	}

	return tmpFilePath, nil
}

func saveWebCatalog(srcFilePath string, destDirPath string) (time.Time, error) {
	if err := os.MkdirAll(destDirPath, 0700); err != nil {
		return time.Time{}, fmt.Errorf("error creating catalog directory: %w", err)
	}

	destFilePath := path.Join(destDirPath, "catalog.yaml")
	destFileStat, err := os.Stat(destFilePath)
	if err == nil {
		return destFileStat.ModTime(), nil
	}
	if !os.IsNotExist(err) {
		return time.Time{}, fmt.Errorf("error saving catalog: %w", err)
	}

	destFile, err := os.Create(destFilePath)
	defer destFile.Close()
	if err != nil {
		return time.Time{}, fmt.Errorf("error opening catalog json file: %w", err)
	}

	srcFile, err := os.Open(srcFilePath)
	defer srcFile.Close()
	if err != nil {
		return time.Time{}, fmt.Errorf("error opening catalog json file: %w", err)
	}

	_, err = io.Copy(destFile, srcFile)
	if err != nil {
		return time.Time{}, fmt.Errorf("error copying catalog json file: %w", err)
	}

	destFileStat, err = os.Stat(destFilePath)
	if err != nil {
		return time.Time{}, fmt.Errorf("error stating catalog json file: %w", err)
	}

	return destFileStat.ModTime(), nil
}
