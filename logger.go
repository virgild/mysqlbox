package mysqlbox

import (
	"bytes"
	"log"
)

type mysqlLogger struct {
	buf *bytes.Buffer
	lg  *log.Logger
}

func newMySQLLogger(buf *bytes.Buffer) *mysqlLogger {
	lg := log.New(buf, "mysql: ", 0)
	lg.SetOutput(buf)
	ml := &mysqlLogger{
		buf: buf,
		lg:  lg,
	}
	return ml
}

func (l *mysqlLogger) Print(args ...interface{}) {
	l.lg.Print(args[0])
}
