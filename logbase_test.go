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

/*
func TestSaveKeyValue(t *testing.T) {
    lbase, err := Open(lbname)
	if err != nil {
		t.Fatalf("Could not open test logbase: %s", err)
	}

    k := "key"
    v := []byte("value")
    err = lbase.Put(k, v)
	if err != nil {
		t.Fatalf("Could not put key value pair into test logbase: %s", err)
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
		t.Fatalf("Could not retrieve key value pair from test logbase: %s", err)
	}
    v := string(result)
    if v != expected {
		t.Fatalf(fmt.Sprintf(
            "The retrieved value %q differed from the expected value %q: %s",
            v, expected),
            err)
    }
}
*/

// Put and get a key-value pair in a single "session".
func TestSaveRetrieveKeyValue(t *testing.T) {
    lbase, err := Open(lbname)
	if err != nil {
		t.Fatalf("Could not open test logbase: %s", err)
	}

    key := "key"
    val := []byte("value")

    errput := lbase.Put(key, val)
	if errput != nil {
		t.Fatalf("Could not put key value pair into test logbase: %s", errput)
	}

    got, errget := lbase.Get(key)
	if errget != nil {
		t.Fatalf("Could not get key value pair from test logbase: %s", errget)
	}

    gotstr := string(got)
    valstr := string(val)
    if valstr != gotstr {
		t.Fatalf(fmt.Sprintf(
            "The retrieved value %q differed from the expected value %q: %s",
            gotstr, valstr),
            err)
    }
}
