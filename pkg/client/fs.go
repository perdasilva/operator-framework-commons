package client

import (
	"context"
	"fmt"
	"io"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Client struct {
	baseDir string
	scheme  *runtime.Scheme
	codecs  serializer.CodecFactory
}

func NewClient(baseDir string, scheme *runtime.Scheme) *Client {
	return &Client{
		baseDir: baseDir,
		scheme:  scheme,
		codecs:  serializer.NewCodecFactory(scheme),
	}
}

func (c *Client) getResourcePath(gvk schema.GroupVersionKind, namespace, name string) string {
	if namespace == "" {
		namespace = "cluster"
	}
	return filepath.Join(c.baseDir, namespace, gvk.Group, gvk.Version, gvk.Kind, name+".yaml")
}

func (c *Client) readFromFile(path string, obj runtime.Object) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	decoder := c.codecs.UniversalDeserializer()
	_, _, err = decoder.Decode(data, nil, obj)
	return err
}

func (c *Client) writeToFile(path string, obj runtime.Object) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := c.codecs.LegacyCodec(obj.GetObjectKind().GroupVersionKind().GroupVersion())
	data, err := runtime.Encode(encoder, obj)
	if err != nil {
		return err
	}

	_, err = file.Write(data)
	return err
}

func (c *Client) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	gvks, _, err := c.scheme.ObjectKinds(obj)
	if err != nil || len(gvks) == 0 {
		return fmt.Errorf("failed to resolve GVK: %w", err)
	}

	gvk := gvks[0]
	path := c.getResourcePath(gvk, key.Namespace, key.Name)
	if err := c.readFromFile(path, obj); err != nil {
		if os.IsNotExist(err) {
			groupResource := schema.GroupResource{
				Group:    gvk.Group,
				Resource: gvk.Kind,
			}
			return errors.NewNotFound(groupResource, key.Name)
		}
		return err
	}

	obj.GetObjectKind().SetGroupVersionKind(gvk)
	return nil
}

func (c *Client) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	gvks, _, err := c.scheme.ObjectKinds(list)
	if err != nil || len(gvks) == 0 {
		return fmt.Errorf("failed to resolve GVK: %w", err)
	}

	listGVK := gvks[0]
	itemGVK := listGVK.GroupVersion().WithKind(strings.TrimSuffix(listGVK.Kind, "List"))
	// once we support options, we can set whether namespaced or cluster
	baseDir := filepath.Join(c.baseDir, "cluster", itemGVK.Group, itemGVK.Version, itemGVK.Kind)

	files, err := filepath.Glob(filepath.Join(baseDir, "*.yaml"))
	if err != nil {
		return err
	}

	items := reflect.ValueOf(list).Elem().FieldByName("Items")
	for _, file := range files {
		item, err := c.scheme.New(itemGVK)
		if err != nil {
			return err
		}
		if err := c.readFromFile(file, item); err != nil {
			return err
		}

		items.Set(reflect.Append(items, reflect.ValueOf(item).Elem()))
	}

	list.GetObjectKind().SetGroupVersionKind(listGVK)
	return nil
}

func (c *Client) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	gvks, _, err := c.scheme.ObjectKinds(obj)
	if err != nil || len(gvks) == 0 {
		return fmt.Errorf("failed to resolve GVK: %w", err)
	}

	gvk := gvks[0]
	path := c.getResourcePath(gvk, obj.GetNamespace(), obj.GetName())

	// Ensure the file does not already exist
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("resource already exists: %s", path)
	}

	return c.writeToFile(path, obj)
}

func (c *Client) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	gvks, _, err := c.scheme.ObjectKinds(obj)
	if err != nil || len(gvks) == 0 {
		return fmt.Errorf("failed to resolve GVK: %w", err)
	}

	gvk := gvks[0]
	path := c.getResourcePath(gvk, obj.GetNamespace(), obj.GetName())
	return c.writeToFile(path, obj)
}

func (c *Client) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	gvks, _, err := c.scheme.ObjectKinds(obj)
	if err != nil || len(gvks) == 0 {
		return fmt.Errorf("failed to resolve GVK: %w", err)
	}

	gvk := gvks[0]
	path := c.getResourcePath(gvk, obj.GetNamespace(), obj.GetName())
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return client.IgnoreNotFound(err)
		}
		return err
	}

	return nil
}
