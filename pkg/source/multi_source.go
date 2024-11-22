package source

import (
	"context"
	"fmt"
	v1alpha1 "github.com/operator-framework/catalogd/api/v1"
	"io/fs"
)

var _ Unpacker = &MultiSourceUnpacker{}

type MultiSourceUnpacker struct {
	Router map[v1alpha1.SourceType]Unpacker
}

func (m MultiSourceUnpacker) GetContentFS(ctx context.Context, catalog *v1alpha1.ClusterCatalog) (fs.FS, error) {
	unpacker, ok := m.Router[catalog.Spec.Source.Type]
	if !ok {
		return nil, fmt.Errorf("no unpacker for source type %q", catalog.Spec.Source.Type)
	}
	return unpacker.GetContentFS(ctx, catalog)
}

func (m MultiSourceUnpacker) Unpack(ctx context.Context, catalog *v1alpha1.ClusterCatalog) (*Result, error) {
	unpacker, ok := m.Router[catalog.Spec.Source.Type]
	if !ok {
		return nil, fmt.Errorf("no unpacker for source type %q", catalog.Spec.Source.Type)
	}
	return unpacker.Unpack(ctx, catalog)
}

func (m MultiSourceUnpacker) Cleanup(ctx context.Context, catalog *v1alpha1.ClusterCatalog) error {
	unpacker, ok := m.Router[catalog.Spec.Source.Type]
	if !ok {
		return fmt.Errorf("no unpacker for source type %q", catalog.Spec.Source.Type)
	}
	return unpacker.Cleanup(ctx, catalog)
}
