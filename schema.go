package mysqlbox

import (
	"bytes"
	"io"
)

type InitialSchema struct {
	buf    *bytes.Buffer
	reader io.Reader
}

func InitialSchemaFromReader(reader io.Reader) *InitialSchema {
	return &InitialSchema{
		reader: reader,
	}
}

func InitialSchemaFromBuffer(buf []byte) *InitialSchema {
	return &InitialSchema{
		buf: bytes.NewBuffer(buf),
	}
}
