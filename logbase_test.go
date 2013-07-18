package logbase

import (
	"testing"
    //"fmt"
    "os"
)

const lbtest = "test"
const debuglog = "debug.log"

func TestCreateLogbase(t *testing.T) {
    err := os.RemoveAll(lbtest)
	if err != nil {t.Fatalf("Trouble deleting dir " + lbtest + ": %s", err)}

    lbase := MakeLogbase(lbtest, NilLogger()).Init()

	if lbase.err != nil {
		t.Fatalf("Could not create test logbase: %s", lbase.err)
	}
}

// Put and get a key-value pair into a virgin logbase.
func TestSaveRetrieveKeyValue1(t *testing.T) {
    k := GenerateRandomHexStr(10)
    v := GenerateRandomHexStr(10)
    saveRetrieveKeyValue(lbtest, k, v, t)
}

// Put and get a key-value pair into an existing logbase.
func TestSaveRetrieveKeyValue2(t *testing.T) {
    k := GenerateRandomHexStr(10)
    v := GenerateRandomHexStr(10)
    saveRetrieveKeyValue(lbtest, k, v, t)
}

// Put and get a key-value pair into an existing logbase.
func TestSaveRetrieveKeyValue3(t *testing.T) {
    k := GenerateRandomHexStr(10)
    v := GenerateRandomHexStr(10)
    saveRetrieveKeyValue(lbtest, k, v, t)
}

// Put and get a key-value pair.
func saveRetrieveKeyValue(lbname, keystr, valstr string, t *testing.T) {
    lbase := MakeLogbase(lbname, ScreenLogger().SetLevel("FINE")).Init()
	if lbase.err != nil {
		t.Fatalf("Could not open test logbase: %s", lbase.err)
	}

    key := keystr
    val := []byte(valstr)

    lbase.Put(key, val)
	if lbase.err != nil {
		t.Fatalf("Could not put key value pair into test logbase: %s", lbase.err)
	}

    got, errget := lbase.Get(key)
	if errget != nil {
		t.Fatalf("Could not get key value pair from test logbase: %s", errget)
	}

    gotstr := string(got)
    vstr := string(val)
    if vstr != gotstr {
		t.Fatalf("The retrieved value %q differed from the expected value %q",
            gotstr, vstr)
    }
}
