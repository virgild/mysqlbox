package mysqlbox

import (
	"bytes"
	"io"
)

// Data contains data.
type Data struct {
	buf    *bytes.Buffer
	reader io.Reader
}

// DataFromReader can be used to load data from a reader object.
func DataFromReader(reader io.Reader) *Data {
	return &Data{
		reader: reader,
	}
}

// DataFromBuffer can be used to load data from a byte array.
func DataFromBuffer(buf []byte) *Data {
	return &Data{
		buf: bytes.NewBuffer(buf),
	}
}
