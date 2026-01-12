package mount

import (
	"fmt"
	"io"
)

type BaseStreamer struct {
	Streamer
}

func (*BaseStreamer) CanRead() bool {
	return false
}

func (*BaseStreamer) CanWrite() bool {
	return false
}

func (*BaseStreamer) IsBusy() bool {
	return false
}

func (*BaseStreamer) Close() error {
	return nil
}

func (*BaseStreamer) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("not implemented")
}

func (*BaseStreamer) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("not implemented")
}

func (*BaseStreamer) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (*BaseStreamer) ReadFrom(r io.Reader) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (*BaseStreamer) WriteTo(w io.Writer) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}
