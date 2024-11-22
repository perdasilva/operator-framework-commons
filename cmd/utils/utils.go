package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	v1alpha1 "github.com/operator-framework/catalogd/api/v1"
	ocv1 "github.com/operator-framework/operator-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"net/url"
	"strings"
)

func GetSourceUrlFromClusterCatalog(catalog *v1alpha1.ClusterCatalog) (string, error) {
	if catalog == nil {
		return "", nil
	}
	switch catalog.Spec.Source.Type {
	case v1alpha1.SourceTypeWeb:
		return catalog.Spec.Source.Web.URL, nil
	case v1alpha1.SourceTypeImage:
		return catalog.Spec.Source.Image.Ref, nil
	default:
		return "", fmt.Errorf(`source type "%s" is not supported`, catalog.Spec.Source.Type)
	}
}

func MakeCatalogID(input string) string {
	hasher := sha256.New()
	hasher.Write([]byte(input))
	hashBytes := hasher.Sum(nil)
	hashString := hex.EncodeToString(hashBytes)
	return fmt.Sprintf("%s", hashString[0:8])
}

func BuildClusterCatalogFromUrlString(catalogUrl string) (*v1alpha1.ClusterCatalog, error) {
	parsedUrl, err := url.Parse(catalogUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse catalog url: %w", err)
	}
	var catalogSource v1alpha1.CatalogSource
	switch parsedUrl.Scheme {
	case "http", "https":
		catalogSource = v1alpha1.CatalogSource{
			Type: v1alpha1.SourceTypeWeb,
			Web: &v1alpha1.WebSource{
				URL: parsedUrl.String(),
			},
		}
	case "oci":
		catalogSource = v1alpha1.CatalogSource{
			Type: v1alpha1.SourceTypeImage,
			Image: &v1alpha1.ImageSource{
				// Remove oci:// prefix
				Ref: strings.TrimPrefix(parsedUrl.String(), "oci://"),
			},
		}
	default:
		return nil, fmt.Errorf("catalog url scheme %s is not supported", parsedUrl.Scheme)
	}

	return &v1alpha1.ClusterCatalog{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ClusterCatalog",
			APIVersion: v1alpha1.GroupVersion.Identifier(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              MakeCatalogID(parsedUrl.String()),
			CreationTimestamp: metav1.Now(),
			Generation:        1,
		},
		Spec: v1alpha1.ClusterCatalogSpec{
			Source: catalogSource,
		},
	}, nil
}

func BuildClusterExtension(packageName string) *ocv1.ClusterExtension {
	return &ocv1.ClusterExtension{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ClusterExtension",
			APIVersion: ocv1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              packageName,
			CreationTimestamp: metav1.Now(),
		},
		Spec: ocv1.ClusterExtensionSpec{
			Namespace: "namespace",
			ServiceAccount: ocv1.ServiceAccountReference{
				Name: "service-account",
			},
			Source: ocv1.SourceConfig{
				SourceType: ocv1.SourceTypeCatalog,
				Catalog: &ocv1.CatalogSource{
					PackageName: packageName,
				},
			},
		},
	}
}
