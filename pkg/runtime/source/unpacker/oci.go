package unpacker

import (
	"context"
	"errors"
	"fmt"
	"github.com/containers/image/v5/docker"
	"github.com/containers/image/v5/docker/reference"
	"github.com/containers/image/v5/manifest"
	"github.com/containers/image/v5/pkg/blobinfocache/none"
	"github.com/perdasilva/operator-framework-commons/pkg/runtime/source"
	"github.com/perdasilva/operator-framework-commons/pkg/runtime/source/progress"
	"io"
	"os"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"

	"github.com/containerd/containerd/archive"
	"github.com/containers/image/v5/copy"
	"github.com/containers/image/v5/oci/layout"
	"github.com/containers/image/v5/pkg/compression"
	"github.com/containers/image/v5/signature"
	"github.com/containers/image/v5/types"
)

const (
	TaskIDOCIResolve = "Resolve"
	TaskIDOCIPull    = "Pull"
	TaskIDOCIUnpack  = "Unpack"
)

type OCIUnpacker struct {
	SourceContextFunc func() (*types.SystemContext, error)
	ArchiveFilter     archive.Filter
}

func (i *OCIUnpacker) Unpack(ctx context.Context, imgRef reference.Named, dest string, opts *source.Options) error {
	srcCtx, err := i.SourceContextFunc()
	if err != nil {
		return err
	}

	progressEmitter := progress.Emitter{
		Event:        progress.Event{},
		ProgressChan: opts.ProgressChan,
	}
	//////////////////////////////////////////////////////
	//
	// Resolve a canonical reference for the image.
	//
	//////////////////////////////////////////////////////
	progressEmitter.NewTask(TaskIDOCIResolve).WithSize(-1).Emit()
	canonicalRef, err := resolveCanonicalRef(ctx, imgRef, srcCtx)
	if err != nil {
		return err
	}
	progressEmitter.Done().Emit()

	//////////////////////////////////////////////////////
	//
	// Check if the image is already unpacked. If it is,
	// return the unpacked directory.
	//
	//////////////////////////////////////////////////////
	unpackPath := filepath.Join(dest, canonicalRef.Digest().String())
	if unpackStat, err := os.Stat(unpackPath); err == nil {
		if !unpackStat.IsDir() {
			panic(fmt.Sprintf("unexpected file at unpack path %q: expected a directory", unpackPath))
		}
		progressEmitter.NewTask(TaskIDOCIPull).Skip().Emit()
		progressEmitter.NewTask(TaskIDOCIUnpack).Skip().Emit()
		return nil
	}

	//////////////////////////////////////////////////////
	//
	// Create a docker reference for the source and an OCI
	// layout reference for the destination, where we will
	// temporarily store the image in order to unpack it.
	//
	// We use the OCI layout as a temporary storage because
	// copy.Image can concurrently pull all the layers.
	//
	//////////////////////////////////////////////////////
	dockerRef, err := docker.NewReference(imgRef)
	if err != nil {
		return fmt.Errorf("error creating source reference: %w", err)
	}

	layoutDir, err := os.MkdirTemp("", fmt.Sprintf("oci-layout"))
	if err != nil {
		return fmt.Errorf("error creating temporary directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(layoutDir); err != nil {
			// l.Error(err, "error removing temporary OCI layout directory")
		}
	}()

	layoutRef, err := layout.NewReference(layoutDir, canonicalRef.String())
	if err != nil {
		return fmt.Errorf("error creating reference: %w", err)
	}

	err = pullImage(ctx, srcCtx, dockerRef, layoutRef, progressEmitter)
	if err != nil {
		return err
	}

	//////////////////////////////////////////////////////
	//
	// Mount the image we just pulled
	//
	//////////////////////////////////////////////////////
	return i.unpackImage(ctx, unpackPath, layoutRef, srcCtx, progressEmitter)
}

func (i *OCIUnpacker) unpackImage(ctx context.Context, unpackPath string, imageReference types.ImageReference, sourceContext *types.SystemContext, progressEmitter progress.Emitter) error {
	img, err := imageReference.NewImage(ctx, sourceContext)
	if err != nil {
		return fmt.Errorf("error reading image: %w", err)
	}
	defer func() {
		if err := img.Close(); err != nil {
			panic(err)
		}
	}()

	layoutSrc, err := imageReference.NewImageSource(ctx, sourceContext)
	if err != nil {
		return fmt.Errorf("error creating image source: %w", err)
	}

	if err := os.MkdirAll(unpackPath, 0700); err != nil {
		return fmt.Errorf("error creating unpack directory: %w", err)
	}

	var applyOpts []archive.ApplyOpt
	if i.ArchiveFilter != nil {
		applyOpts = append(applyOpts, archive.WithFilter(i.ArchiveFilter))
	}

	progressEmitter.NewTask(TaskIDOCIUnpack).WithSize(0).WithSubjectID("").Emit()
	defer progressEmitter.WithSubjectID("").WithProgress(0, 0).Done().Emit()
	for i, layerInfo := range img.LayerInfos() {
		if err := func() error {
			progressEmitter.WithSubjectID(layerInfo.Digest.String()[7:15]).WithSize(layerInfo.Size)
			layerReader, _, err := layoutSrc.GetBlob(ctx, layerInfo, none.NoCache)
			if err != nil {
				return fmt.Errorf("error getting blob for layer[%d]: %w", i, err)
			}
			defer layerReader.Close()

			progressReader := progress.NewProgressReader(layerReader, func(n int64, total int64) {
				progressEmitter.WithProgress(total, n).Progress().Emit()
			})

			if err := applyLayer(ctx, unpackPath, progressReader, applyOpts...); err != nil {
				return fmt.Errorf("error applying layer[%d]: %w", i, err)
			}
			return nil
		}(); err != nil {
			return errors.Join(err, deleteRecursive(unpackPath))
		}
	}
	progressEmitter.NewTask(TaskIDOCIUnpack).WithSubjectID("").Done().Emit()
	if err := setReadOnlyRecursive(unpackPath); err != nil {
		return fmt.Errorf("error making unpack directory read-only: %w", err)
	}
	return nil
}

func pullImage(ctx context.Context, srcCtx *types.SystemContext, srcRef types.ImageReference, destRef types.ImageReference, progressEmitter progress.Emitter) error {
	//////////////////////////////////////////////////////
	//
	// Load an image signature policy and build
	// a policy context for the image pull.
	//
	//////////////////////////////////////////////////////
	policyContext, err := loadPolicyContext(srcCtx)
	if err != nil {
		return fmt.Errorf("error loading policy context: %w", err)
	}
	defer func() {
		_ = policyContext.Destroy()
	}()

	//////////////////////////////////////////////////////
	//
	// Pull the image from the source to the destination
	//
	//////////////////////////////////////////////////////
	var progressChan chan types.ProgressProperties = nil
	done := make(chan struct{})
	defer close(done)

	if progressEmitter.ProgressChan != nil {
		progressChan = make(chan types.ProgressProperties)
		go func() {
			defer func() {
				done <- struct{}{}
			}()
			for {
				select {
				case <-ctx.Done():
					return
				case evt, hasNext := <-progressChan:
					if !hasNext {
						return
					}

					progressEmitter.
						WithSubjectID(evt.Artifact.Digest.String()[7:15]).
						WithSize(evt.Artifact.Size).
						WithProgress(int64(evt.Offset), int64(evt.OffsetUpdate))

					switch evt.Event {
					case types.ProgressEventDone:
					case types.ProgressEventRead:
						progressEmitter.Progress().Emit()
					case types.ProgressEventNewArtifact:
						progressEmitter.Progress().Emit()
					case types.ProgressEventSkipped:
						progressEmitter.Skip().Emit()
					default:
						panic("unknown progress event")
					}
				}
			}
		}()
	}
	progressEmitter.NewTask(TaskIDOCIPull).WithSize(0).Emit()
	_, err = copy.Image(ctx, policyContext, destRef, srcRef, &copy.Options{
		SourceCtx: srcCtx,
		// We use the OCI layout as a temporary storage and
		// pushing signatures for OCI images is not supported
		// so we remove the source signatures when copying.
		// Signature validation will still be performed
		// accordingly to a provided policy context.
		RemoveSignatures: true,
		Progress:         progressChan,
		ProgressInterval: 100 * time.Millisecond,
	})
	progressEmitter.Done().Emit()

	if progressChan != nil {
		close(progressChan)
		<-done
	}

	return err
}

func loadPolicyContext(sourceContext *types.SystemContext) (*signature.PolicyContext, error) {
	policy, err := signature.DefaultPolicy(sourceContext)
	if os.IsNotExist(err) {
		policy, err = signature.NewPolicyFromBytes([]byte(`{"default":[{"type":"insecureAcceptAnything"}]}`))
	}
	if err != nil {
		return nil, fmt.Errorf("error loading default policy: %w", err)
	}
	return signature.NewPolicyContext(policy)
}

func applyLayer(ctx context.Context, unpackPath string, layer io.ReadCloser, opts ...archive.ApplyOpt) error {
	decompressed, _, err := compression.AutoDecompress(layer)
	if err != nil {
		return fmt.Errorf("auto-decompress failed: %w", err)
	}
	defer decompressed.Close()
	_, err = archive.Apply(ctx, unpackPath, decompressed, opts...)
	return err
}

//func applyLayerFilter() archive.Filter {
//	return func(h *tar.Header) (bool, error) {
//		h.Uid = os.Getuid()
//		h.Gid = os.Getgid()
//		h.Mode |= 0700
//		return true, nil
//	}
//}

func setReadOnlyRecursive(root string) error {
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		fi, err := d.Info()
		if err != nil {
			return err
		}

		if err := func() error {
			switch typ := fi.Mode().Type(); typ {
			case os.ModeSymlink:
				// do not follow symlinks
				// 1. if they resolve to other locations in the root, we'll find them anyway
				// 2. if they resolve to other locations outside the root, we don't want to change their permissions
				return nil
			case os.ModeDir:
				return os.Chmod(path, 0500)
			case 0: // regular file
				return os.Chmod(path, 0400)
			default:
				return fmt.Errorf("refusing to change ownership of file %q with type %v", path, typ.String())
			}
		}(); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return fmt.Errorf("error making bundle cache read-only: %w", err)
	}
	return nil
}

func deleteRecursive(root string) error {
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		if err := os.Chmod(path, 0700); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return fmt.Errorf("error making bundle cache writable for deletion: %w", err)
	}
	return os.RemoveAll(root)
}

func resolveCanonicalRef(ctx context.Context, imgRef reference.Named, imageCtx *types.SystemContext) (reference.Canonical, error) {
	if canonicalRef, ok := imgRef.(reference.Canonical); ok {
		return canonicalRef, nil
	}

	srcRef, err := docker.NewReference(imgRef)
	if err != nil {
		return nil, reconcile.TerminalError(fmt.Errorf("error creating reference: %w", err))
	}

	imgSrc, err := srcRef.NewImageSource(ctx, imageCtx)
	if err != nil {
		return nil, fmt.Errorf("error creating image source: %w", err)
	}
	defer imgSrc.Close()

	imgManifestData, _, err := imgSrc.GetManifest(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("error getting manifest: %w", err)
	}
	imgDigest, err := manifest.Digest(imgManifestData)
	if err != nil {
		return nil, fmt.Errorf("error getting digest of manifest: %w", err)
	}
	canonicalRef, err := reference.WithDigest(reference.TrimNamed(imgRef), imgDigest)
	if err != nil {
		return nil, fmt.Errorf("error creating canonical reference: %w", err)
	}
	return canonicalRef, nil
}
