package mysqlbox

import (
	"testing"
)

func TestRandomID(t *testing.T) {
	var idMap = make(map[string]bool)
	for n := 0; n < 256; n++ {
		id := randomID()
		if idMap[id] == true {
			t.Errorf("id %s already exists", id)
			t.FailNow()
		}
		idMap[id] = true
	}
}
