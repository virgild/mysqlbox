package mysqlbox

import (
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var charset = []rune("abcdefghijklmnopqrstuvwxyz0123456789")
var charsetLen = len(charset)

func randomID() string {
	return randStr(5)
}

func randStr(length int) string {
	c := make([]rune, length)
	for n := range c {
		c[n] = charset[rand.Intn(charsetLen)] // #nosec G404
	}

	return string(c)
}
