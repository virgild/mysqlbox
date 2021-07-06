package mysqlbox

import (
	"bytes"
	"fmt"
	"io"
	"os"
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

// DataFromFile can be used to load data from a file.
func DataFromFile(filename string) *Data {
	f, err := os.Open(filename)
	if err != nil {
		panic(fmt.Sprintf("error opening file %s: %s", filename, err.Error()))
	}
	defer f.Close()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, f)
	if err != nil {
		panic(err)
	}

	return DataFromBuffer(buf.Bytes())
}
