package logbase

import (
	"testing"
    "fmt"
    "os"
)

const lbname = "test"
const debuglog = "debug.log"

func TestCreateLogbase(t *testing.T) {
    err := os.RemoveAll(lbname)
	if err != nil {t.Fatalf("Trouble deleting dir " + lbname + ": %s", err)}
    err = os.Remove(debuglog)
	if err != nil {t.Fatalf("Trouble deleting file " + debuglog + ": %s", err)}

    _, err = Open(lbname)

	if err != nil {
		t.Fatalf("Could not create test logbase: %s", err)
	}
}

func TestSaveKeyValue(t *testing.T) {
    lbase, err := Open(lbname)
	if err != nil {
		t.Fatalf("Could not open test logbase: %s", err)
	}

    k := "key"
    v := []byte("value")
    err = lbase.Put(k, v)
	if err != nil {
		t.Fatalf("Could not put key value pair into logbase: %s", err)
	}
}

func TestRetrieveKeyValue(t *testing.T) {
    lbase, err := Open(lbname)
	if err != nil {
		t.Fatalf("Could not open test logbase: %s", err)
	}

    k := "key"
    expected := "value"
    var result []byte
    result, err = lbase.Get(k)
	if err != nil {
		t.Fatalf("Could not put key value pair into logbase: %s", err)
	}
    v := string(result)
    if v != expected {
		t.Fatalf(fmt.Sprintf(
            "The retrieved value %q differed from the expected value %q: %s",
            v, expected),
            err)
    }
}
