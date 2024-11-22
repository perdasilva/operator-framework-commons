package progress

import "io"

type progressReader struct {
	io.ReadCloser
	callback func(bytesRead int64, totalRead int64)
	total    int64
}

func (p *progressReader) Read(buf []byte) (int, error) {
	n, err := p.ReadCloser.Read(buf)
	p.total += int64(n)
	p.callback(int64(n), p.total)
	return n, err
}

func NewProgressReader(r io.ReadCloser, callback func(int64, int64)) io.ReadCloser {
	return &progressReader{
		ReadCloser: r,
		callback:   callback,
	}
}
