package mysqlbox

import (
	"math/rand"
	"time"

	"github.com/docker/docker/pkg/namesgenerator"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func randomID() string {
	return namesgenerator.GetRandomName(1)
}
