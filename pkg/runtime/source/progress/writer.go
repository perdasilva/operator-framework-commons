package progress

import (
	"context"
	"sync"
)

type Renderer struct {
	renderFn func(evt Event) error
	mu       sync.RWMutex
	exit     chan struct{}
	err      error
}

func (r *Renderer) Start(ctx context.Context) (chan<- Event, error) {
	r.mu.Lock()
	r.mu.Unlock()
	progressChan := make(chan Event)
	go func() {
		for {
			select {
			case <-r.exit:
				return
			case <-ctx.Done():
				return
			case evt, ok := <-progressChan:
				if !ok {
					return
				}
				if err := r.renderFn(evt); err != nil {
					return
				}
			}
		}
	}()

	return progressChan, nil
}
func (r *Renderer) Stop() {

}
func (r *Renderer) Wait() {}
