package mysqlbox

import (
	"bytes"
	"io"
)

// InitialSchema contains an SQL provided to MySQLBox that will be run when the MySQL container runs.
type InitialSchema struct {
	buf    *bytes.Buffer
	reader io.Reader
}

// InitialSchemaFromReader loads the SQL script from a reader object.
func InitialSchemaFromReader(reader io.Reader) *InitialSchema {
	return &InitialSchema{
		reader: reader,
	}
}

// InitialSchemaFromBuffer loads the SQL script from a byte array.
func InitialSchemaFromBuffer(buf []byte) *InitialSchema {
	return &InitialSchema{
		buf: bytes.NewBuffer(buf),
	}
}
