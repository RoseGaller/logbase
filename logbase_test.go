package logbase

import (
	"testing"
    //"fmt"
    "os"
)

const lbtest = "test"
var lbase *Logbase = setupLogbase()
var k, v []string
var pair int = 0
var mc *MasterCatalog
var zm *Zapmap

// Put and get a key-value pair into a virgin logbase.
func TestSaveRetrieveKeyValue1(t *testing.T) {
    k, v = generateRandomKeyValuePairs(20,3,10)
    saveRetrieveKeyValue(k[pair], v[pair], t)
}

// Put and get a key-value pair into an existing logbase.
func TestSaveRetrieveKeyValue2(t *testing.T) {
    pair++
    saveRetrieveKeyValue(k[pair], v[pair], t)
}

// Put and get a key-value pair 3 times and ensure that the zapmap is being
// properly filled with stale value locations.
func TestSaveRetrieveKeyValue3(t *testing.T) {
    pair++
    saveRetrieveKeyValue(k[pair], v[pair], t)
    mcr := make([]*MasterCatalogRecord, 3)
    mcr[0] = lbase.mcat.index[k[pair]]
    saveRetrieveKeyValue(k[pair], v[pair], t)
    mcr[1] = lbase.mcat.index[k[pair]]
    saveRetrieveKeyValue(k[pair], v[pair], t)
    mcr[2] = lbase.mcat.index[k[pair]]
    //dumpIndex(lbase.livelog.indexfile)
    dumpMaster()
    dumpZapmap()
    lbase.Save()
    zrecs := lbase.zmap.zapmap[k[pair]]
    if len(zrecs) != 2 {
		t.Fatalf("The zapmap should contain precisely 2 entries")
	}
    zrec0 := NewZapRecord()
    zrec0.FromMasterCatalogRecord(k[pair], mcr[0])
    zrec1 := NewZapRecord()
    zrec1.FromMasterCatalogRecord(k[pair], mcr[1])
    matches := zrec0.Equals(zrecs[0]) && zrec1.Equals(zrecs[1])
    if !matches {
		t.Fatalf("The zapmap should contain {%s%s} but is instead {%s%s}",
            zrec0,
            zrec1,
            zrecs[0],
            zrecs[1])
	}

    mc = lbase.mcat
    zm = lbase.zmap
}

// Re-initialise the logbase and ensure that the master catalog and zapmap are
// properly loaded.
func TestLoadMasterAndZapmap(t *testing.T) {
    lbase.mcat = NewMasterCatalog()
    lbase.zmap = NewZapmap()
    lbase.Init()
    dumpMaster()
    dumpZapmap()
    if len(lbase.mcat.index) != len(mc.index) {
		t.Fatalf(
            "The loaded master file should have %d entries, but has %d",
            len(mc.index),
            len(lbase.mcat.index))
	}
    if len(lbase.zmap.zapmap) != len(zm.zapmap) {
		t.Fatalf(
            "The loaded zapmap file should have %d entries, but has %d",
            len(zm.zapmap),
            len(lbase.zmap.zapmap))
	}
    for key, mcr := range mc.index {
        if !mcr.Equals(lbase.mcat.index[key]) {
		    t.Fatalf(
                "The saved and loaded master file entry for key %q should " +
                "be %s but is %s",
                key,
                mcr,
                lbase.mcat.index[key])
        }
	}
    for key, zrecs := range zm.zapmap {
        for i, zrec := range zrecs {
            if !zrec.Equals(lbase.zmap.zapmap[key][i]) {
		        t.Fatalf(
                    "The saved and loaded zap file list for key %q should " +
                    "be %s at position %d but is %s",
                    key,
                    zrec,
                    i,
                    lbase.zmap.zapmap[key][i])
            }
	    }
    }
}

// Re-initialise the logbase but this time delete the master catalog and zapmap
// files, forcing master catalog and zapmap reconstruction.
func TestReconstructMasterAndZapmap(t *testing.T) {
    path := lbase.mcat.file.abspath
    err := os.RemoveAll(path)
	if err != nil {WrapError("Trouble deleting dir " + path, err).Fatal()}
    path = lbase.zmap.file.abspath
    err = os.RemoveAll(path)
	if err != nil {WrapError("Trouble deleting dir " + path, err).Fatal()}
    lbase.mcat = NewMasterCatalog()
    lbase.zmap = NewZapmap()
    lbase.Init()
    dumpMaster()
    dumpZapmap()
    if len(lbase.mcat.index) != len(mc.index) {
		t.Fatalf(
            "The loaded master file should have %d entries, but has %d",
            len(mc.index),
            len(lbase.mcat.index))
	}
    if len(lbase.zmap.zapmap) != len(zm.zapmap) {
		t.Fatalf(
            "The loaded zapmap file should have %d entries, but has %d",
            len(zm.zapmap),
            len(lbase.zmap.zapmap))
	}
    for key, mcr := range mc.index {
        if !mcr.Equals(lbase.mcat.index[key]) {
		    t.Fatalf(
                "The saved and loaded master file entry for key %q should " +
                "be %s but is %s",
                key,
                mcr,
                lbase.mcat.index[key])
        }
	}
    for key, zrecs := range zm.zapmap {
        for i, zrec := range zrecs {
            if !zrec.Equals(lbase.zmap.zapmap[key][i]) {
		        t.Fatalf(
                    "The saved and loaded zap file list for key %q should " +
                    "be %s at position %d but is %s",
                    key,
                    zrec,
                    i,
                    lbase.zmap.zapmap[key][i])
            }
	    }
    }
}

// Delete stale data.
func TestZap(t *testing.T) {
    lbase.Zap(5)
}

// SUPPORT FUNCTIONS ==========================================================

// Set up the global test logbase.
func setupLogbase() (lb *Logbase) {
    err := os.RemoveAll(lbtest)
	if err != nil {WrapError("Trouble deleting dir " + lbtest, err).Fatal()}
    lb = MakeLogbase(lbtest, ScreenLogger().SetLevel("FINE"))
    err = lb.Init()
	if err != nil {
		WrapError("Could not create test logbase", err).Fatal()
	}
    lb.config.LOGFILE_MAXBYTES = 100
    return
}

// Dump the file register.
func dumpFileReg() {
    files := lbase.freg.StringArray()
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
    lbase.debug.Fine(DEBUG_DEFAULT, "Index file records for %s:", ifile.abspath)
    for _, irec := range lfindex.list {
        lbase.debug.Fine(DEBUG_DEFAULT, irec.String())
    }
}

// Dump contents of the (internal) master catalog.
func dumpMaster() {
    lbase.debug.Fine(DEBUG_DEFAULT, "Master catalog records:")
    for key, mcr := range lbase.mcat.index {
        lbase.debug.Fine(DEBUG_DEFAULT, "%q %s", key, mcr.String())
    }
}

// Dump contents of the internal zapmap.
func dumpZapmap() {
    lbase.debug.Fine(DEBUG_DEFAULT, "Zapmap records:")
    for key, zrecs := range lbase.zmap.zapmap {
        var line string = ""
        for _, zrec := range zrecs {
            line += zrec.String()
        }
        lbase.debug.Fine(DEBUG_DEFAULT, "%q {%s}", key, line)
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

func generateRandomKeyValuePairs(n, min, max uint64) (keys, values []string) {
    keys = GenerateRandomHexStrings(n, min, max)
    values = GenerateRandomHexStrings(n, min, max)
    return
}

func dumpKeyValuePairs(keys, values []string) {
    lbase.debug.Advise(DEBUG_DEFAULT, "Dumping key value pairs:")
    comlen := len(keys)
    if len(values) < len(keys) {comlen = len(values)}
    for i := 0; i < comlen; i++ {
        lbase.debug.Advise(DEBUG_DEFAULT, " (%s,%s)", keys[i], values[i])
    }
    return
}
