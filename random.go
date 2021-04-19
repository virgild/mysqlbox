package mysqlbox

import (
	"crypto/rand"
	"time"

	"github.com/oklog/ulid/v2"
)

var entropy = ulid.Monotonic(rand.Reader, 0)

func randomID() string {
	t := time.Now()
	return ulid.MustNew(ulid.Timestamp(t), entropy).String()
}
