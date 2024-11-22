package runtime

import (
	"fmt"
	"github.com/perdasilva/operator-framework-commons/pkg/runtime/source"
)

type Router struct {
	Router map[source.SourceType]*Runtime
}

func (e *Router) GetRuntimeForBundleSource(bundleSource *source.BundleSource) (*Runtime, error) {
	if _, ok := e.Router[bundleSource.Type]; ok {
		return e.Router[bundleSource.Type], nil
	}
	return nil, fmt.Errorf("unsupported source type: %s", bundleSource.Type)
}
