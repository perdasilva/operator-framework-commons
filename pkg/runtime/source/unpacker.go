package source

import (
	"context"
	"github.com/perdasilva/operator-framework-commons/pkg/runtime/source/progress"
)

type Options struct {
	ProgressChan chan<- progress.Event
}

type Unpacker[T interface{}] interface {
	Unpack(ctx context.Context, src T, dest string, opts *Options) error
}
