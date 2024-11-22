package source

import (
	"context"
)

type Downloader[T interface{}] interface {
	Download(ctx context.Context, src T, dest string, opts Options) error
}
