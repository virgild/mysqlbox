package mysqlbox

import (
	"github.com/docker/docker/pkg/namesgenerator"
)

func randomID() string {
	return namesgenerator.GetRandomName(1)
}
