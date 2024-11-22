/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package main

import (
	"context"
	"fmt"
	catalogd "github.com/operator-framework/catalogd/api/v1"
	ocv1 "github.com/operator-framework/operator-controller/api/v1"
	"github.com/perdasilva/operator-framework-commons/pkg/client"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func main() {
	scheme := runtime.NewScheme()
	_ = catalogd.AddToScheme(scheme)
	_ = ocv1.AddToScheme(scheme)

	cl := client.NewClient("k8s", scheme)
	catalog := &catalogd.ClusterCatalog{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ClusterCatalog",
			APIVersion: "olm.operatorframework.io/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test",
		},
		Spec: catalogd.ClusterCatalogSpec{
			Source: catalogd.CatalogSource{
				Type: catalogd.SourceTypeWeb,
				Web: &catalogd.WebSource{
					URL: "www.google.com",
				},
			},
		},
	}
	fmt.Println(cl.Create(context.Background(), catalog))
}
