package logbase

import (
	"testing"
    //"fmt"
    "os"
)

const lbtest = "test"
var lbase *Logbase = setupLogbase()

// Put and get a key-value pair into a virgin logbase.
func TestSaveRetrieveKeyValue1(t *testing.T) {
    k := GenerateRandomHexStr(10)
    v := GenerateRandomHexStr(10)
    saveRetrieveKeyValue(k, v, t)
}

// Put and get a key-value pair into an existing logbase.
func TestSaveRetrieveKeyValue2(t *testing.T) {
    k := GenerateRandomHexStr(10)
    v := GenerateRandomHexStr(10)
    saveRetrieveKeyValue(k, v, t)
}

// Put and get a key-value pair into an existing logbase.
func TestSaveRetrieveKeyValue3(t *testing.T) {
    k := GenerateRandomHexStr(10)
    v := GenerateRandomHexStr(10)
    saveRetrieveKeyValue(k, v, t)
    dumpIndex(lbase.livelog.indexfile)
    dumpMaster()
}

// SUPPORT FUNCTIONS //////////////////////////////////////////////////////////

// Set up the global test logbase.
func setupLogbase() *Logbase {
    err := os.RemoveAll(lbtest)
	if err != nil {WrapError("Trouble deleting dir " + lbtest, err).Fatal()}
    lb := MakeLogbase(lbtest, ScreenLogger().SetLevel("FINE")).Init()
	if lb.err != nil {
		WrapError("Could not create test logbase", lb.err).Fatal()
	}
    return lb
}

// Dump the file register.
func dumpFileReg() {
    files := lbase.FileRegister.StringArray()
    lbase.debug.Fine(DEBUG_DEFAULT, "Dumping file register:")
    for _, file := range files {
        lbase.debug.Fine(DEBUG_DEFAULT, " " + file)
    }
    return
}

// Dump contents of given index file.
func dumpIndex(ifile *Indexfile) {
    lfindex, err := ifile.Load()
	if err != nil {
		WrapError("Could not load live log index file", err).Fatal()
	}
    lbase.debug.Fine(DEBUG_DEFAULT, "Index file records for %s:", ifile.path)
    for _, irec := range lfindex.list {
        lbase.debug.Fine(DEBUG_DEFAULT, irec.String())
    }
}

// Dump contents of the master catalog.
func dumpMaster() {
    lbase.debug.Fine(DEBUG_DEFAULT, "Master catalog records:")
    for key, mcr := range lbase.index {
        lbase.debug.Fine(DEBUG_DEFAULT, "%q %s", key, mcr.String())
    }
}

// Put and get a key-value pair.
func saveRetrieveKeyValue(keystr, valstr string, t *testing.T) *Logbase {
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

    return lbase
}

