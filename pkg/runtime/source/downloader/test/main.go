package main

import (
	"context"
	"fmt"
	"github.com/cheggaaa/pb/v3"
	"github.com/containers/image/v5/docker/reference"
	"github.com/containers/image/v5/types"
	"github.com/perdasilva/operator-framework-commons/pkg/runtime/source"
	"github.com/perdasilva/operator-framework-commons/pkg/runtime/source/progress"
	"github.com/perdasilva/operator-framework-commons/pkg/runtime/source/unpacker"
	"time"
)

func main() {
	//cursor.Hide()
	//defer cursor.Show()

	ociUnpacker := unpacker.OCIUnpacker{
		SourceContextFunc: func() (*types.SystemContext, error) {
			return &types.SystemContext{}, nil
		},
	}

	ctx := context.Background()
	var progressBar *pb.ProgressBar
	progressChan := make(chan progress.Event)
	barProgressTmpl := ` - [{{cycle . "   " ".  " ".. " "..." }}] {{string . "task" }}{{ bar . " " (red "━") (red "━") (white "━") " "}}{{speed . "%s/s" "? KiB/s"}}`
	noBarProgressTmpl := ` - [{{cycle . "   " ".  " ".. " "..." }}] {{string . "task" }}`
	doneTmpl := ` - [{{(green " ✔ ")}}] {{string . "task" }} ...`
	skipTmpl := ` - [{{(green " ⏭ ")}}] {{string . "task" }} ...`

	taskIdMap := map[string]string{
		unpacker.TaskIDOCIPull:    "💾 Pulling image",
		unpacker.TaskIDOCIResolve: "🔎 Resolving image",
		unpacker.TaskIDOCIUnpack:  "📦 Unpacking image",
	}

	done := make(chan struct{})
	go func() {
		defer func() {
			if progressBar != nil {
				progressBar.Finish()
			}
			done <- struct{}{}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case evt, ok := <-progressChan:
				if !ok {
					return
				}

				switch evt.EventType {
				case progress.EventTypeExit:
					return
				case progress.EventTypeNewTask:
					progressBar = pb.New(int(evt.Size))
					progressBar.Set(pb.Bytes, true)
					progressBar.SetRefreshRate(500 * time.Millisecond)
					progressBar.SetWidth(80)
					if evt.Size < 0 {
						progressBar.SetTemplateString(noBarProgressTmpl)
					} else {
						progressBar.SetTemplateString(barProgressTmpl)
					}
					progressBar.Set("task", taskIdMap[evt.TaskID])
					progressBar.Start()
				case progress.EventTypeDone:
					progressBar.SetTotal(progressBar.Total())
					progressBar.SetTemplateString(doneTmpl)
					progressBar.Finish()
				case progress.EventTypeSkipped:
					progressBar = pb.New(int(evt.Size))
					progressBar.Set(pb.Bytes, true)
					progressBar.SetRefreshRate(500 * time.Millisecond)
					progressBar.SetWidth(80)
					progressBar.SetTemplateString(skipTmpl)
					progressBar.Set("task", taskIdMap[evt.TaskID])
					progressBar.Start()
					progressBar.Add64(evt.Size)
					progressBar.Finish()
				case progress.EventTypeNewProgressed:
					if progressBar == nil {
						continue
					}
					if evt.OffsetUpdate == 0 {
						progressBar.AddTotal(evt.Size)
					} else {
						progressBar.Add64(evt.OffsetUpdate)
					}
				}
			}
		}
	}()
	
	img := "quay.io/operatorhubio/catalog:latest"
	fmt.Printf("Unpacking %s\n\n", img)
	imgRef, _ := reference.ParseNamed(img)
	err := ociUnpacker.Unpack(ctx, imgRef, "test", &source.Options{
		ProgressChan: progressChan,
	})
	progressChan <- progress.Event{
		EventType: progress.EventTypeExit,
	}
	<-done
	close(progressChan)
	if err != nil {
		panic(err)
	}
	fmt.Println("\nDone")
}
