package logbase

import (
	"testing"
)

func TestCreateLogbase(t *testing.T) {
    const lbname = "testbase"
    _, err := Open(lbname)

	if err != nil {
		t.Fatalf("Cannot create test logbase: %s", err)
	}
}
