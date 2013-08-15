package logbase

import (
	"testing"
	//"fmt"
	"os"
	"path/filepath"
)

const (
	lbname = "test"
	logfile_maxbytes = 100
	debug_level = "BASIC"
	user = "admin"
	passhash = "root"
)

var lbtest string
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
	mcr[0] = lbase.mcat.Get(k[pair])
	saveRetrieveKeyValue(k[pair], v[pair], t)
	mcr[1] = lbase.mcat.Get(k[pair])
	saveRetrieveKeyValue(k[pair+1], v[pair+1], t) // mix up a bit
	saveRetrieveKeyValue(k[pair], v[pair], t)
	mcr[2] = lbase.mcat.Get(k[pair])
	//dumpIndex(lbase.livelog.indexfile)
	dumpMaster()
	dumpZapmap()
	err := lbase.Save()
	if err != nil {
		t.Fatalf("Problem saving logbase: %s", err)
	}
	zrecs := lbase.zmap.Get(k[pair])
	if len(zrecs) != 2 {
		t.Fatalf("The zapmap should contain precisely 2 entries")
	}
	zrec0 := NewZapRecord()
	zrec0.FromMasterCatalogRecord(AsLBUINT(len(k[pair]) + LBTYPE_SIZE), mcr[0])
	zrec1 := NewZapRecord()
	zrec1.FromMasterCatalogRecord(AsLBUINT(len(k[pair]) + LBTYPE_SIZE), mcr[1])
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
	lbase.Init(user, passhash)
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
		lbase.debug.Check("key = %v", key)
		lbase.debug.Check("mcr = %v", mcr)
		lbase.debug.Check("lbase.mcat.index[key] = %v", lbase.mcat.Get(key))
		if !mcr.Equals(lbase.mcat.Get(key)) {
		    t.Fatalf(
				"The saved and loaded master file entry for key %q should " +
				"be %s but is %s",
				key,
				mcr,
				lbase.mcat.Get(key))
		}
	}
	for key, zrecs := range zm.zapmap {
		for i, zrec := range zrecs {
			if !zrec.Equals(lbase.zmap.Get(key)[i]) {
		        t.Fatalf(
					"The saved and loaded zap file list for key %q should " +
					"be %s at position %d but is %s",
					key,
					zrec,
					i,
					lbase.zmap.Get(key)[i])
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
	lbase.Init(user, passhash)
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
		if !mcr.Equals(lbase.mcat.Get(key)) {
		    t.Fatalf(
				"The saved and loaded master file entry for key %q should " +
				"be %s but is %s",
				key,
				mcr,
				lbase.mcat.Get(key))
		}
	}
	for key, zrecs := range zm.zapmap {
		for i, zrec := range zrecs {
			if !zrec.Equals(lbase.zmap.Get(key)[i]) {
		        t.Fatalf(
					"The saved and loaded zap file list for key %q should " +
					"be %s at position %d but is %s",
					key,
					zrec,
					i,
					lbase.zmap.Get(key)[i])
			}
	    }
	}
}

// Zap record at start of data sequence.
func TestZapCalcStart(t *testing.T) {
	zpos := []LBUINT{0,14}
	zsz := []LBUINT{4,8}
	size := 40
	cpos, csz := InvertSequence(zpos, zsz, size)
	exppos := []LBUINT{4,22}
	expsz := []LBUINT{10,18}
	if !LBUINTEqual(cpos, exppos) {
		t.Fatalf(
			"While testing zapping a record at the end of a sequence, " +
			"of length %d the zap slices zpos = %v and zsz = %v were " +
			"inverted to cpos = %v and csz = %v, but %v and %v were expected.",
			size, zpos, zsz, cpos, csz, exppos, expsz)
	}
}

// Zap record at end of data sequence.
func TestZapCalcEnd(t *testing.T) {
	zpos := []LBUINT{7,36}
	zsz := []LBUINT{3,4}
	size := 40
	cpos, csz := InvertSequence(zpos, zsz, size)
	exppos := []LBUINT{0,10}
	expsz := []LBUINT{7,26}
	if !LBUINTEqual(cpos, exppos) {
		t.Fatalf(
			"While testing zapping a record at the end of a sequence, " +
			"of length %d the zap slices zpos = %v and zsz = %v were " +
			"inverted to cpos = %v and csz = %v, but %v and %v were expected.",
			size, zpos, zsz, cpos, csz, exppos, expsz)
	}
}

// Zap record midway in data sequence.
func TestZapCalcMid(t *testing.T) {
	zpos := []LBUINT{10,30}
	zsz := []LBUINT{5,5}
	size := 40
	cpos, csz := InvertSequence(zpos, zsz, size)
	exppos := []LBUINT{0,15,35}
	expsz := []LBUINT{10,15,5}
	if !LBUINTEqual(cpos, exppos) {
		t.Fatalf(
			"While testing zapping a record at the end of a sequence, " +
			"of length %d the zap slices zpos = %v and zsz = %v were " +
			"inverted to cpos = %v and csz = %v, but %v and %v were expected.",
			size, zpos, zsz, cpos, csz, exppos, expsz)
	}
}

// Zap records at start, end, and midway including an adjacent pair.
func TestZapCalcKitchenSink1(t *testing.T) {
	zpos := []LBUINT{0,10,23,27,35}
	zsz := []LBUINT{4,6,4,3,5}
	size := 40
	cpos, csz := InvertSequence(zpos, zsz, size)
	exppos := []LBUINT{4,16,30}
	expsz := []LBUINT{6,7,5}
	if !LBUINTEqual(cpos, exppos) {
		t.Fatalf(
			"While testing zapping a record at the end of a sequence, " +
			"of length %d the zap slices zpos = %v and zsz = %v were " +
			"inverted to cpos = %v and csz = %v, but %v and %v were expected.",
			size, zpos, zsz, cpos, csz, exppos, expsz)
	}
}

// Delete stale data.
func TestZap(t *testing.T) {
	dumpLogfiles()
	err := lbase.Zap(5)
	if err != nil {
		t.Fatalf("Problem zapping logfiles: %s", err)
	}
	dumpLogfiles()
	err = lbase.Save()
	if err != nil {
		t.Fatalf("Problem saving logbase: %s", err)
	}
}

// SUPPORT FUNCTIONS ==========================================================

// Set up the global test logbase.
func setupLogbase() (lb *Logbase) {
	cwd, _ := os.Getwd()
	lbtest = filepath.Join(cwd, lbname)
	err := os.RemoveAll(lbtest)
	if err != nil {WrapError("Trouble deleting dir " + lbtest, err).Fatal()}
	lb = MakeLogbase(lbtest, ScreenLogger().SetLevel(debug_level))
	err = lb.Init(user, passhash)
	if err != nil {
		WrapError("Could not create test logbase", err).Fatal()
	}
	lb.config.LOGFILE_MAXBYTES = logfile_maxbytes
	return
}

// Dump the file register.
func dumpFileReg() {
	files := lbase.freg.StringArray()
	lbase.debug.Fine("Dumping file register:")
	for _, file := range files {
		lbase.debug.Fine(" " + file)
	}
	return
}

// Dump contents of given index file.
func dumpIndex(ifile *Indexfile) {
	lfindex, err := ifile.Load()
	if err != nil {
		WrapError("Could not load live log index file", err).Fatal()
	}
	lbase.debug.Fine("Index file records for %s:", ifile.abspath)
	for _, irec := range lfindex.list {
		lbase.debug.Fine(irec.String())
	}
}

// Dump contents of the (internal) master catalog.
func dumpMaster() {
	lbase.debug.Fine("Master catalog records:")
	for key, mcr := range lbase.mcat.index {
		lbase.debug.Fine("%q %s", key, mcr.String())
	}
}

// Dump contents of the internal zapmap.
func dumpZapmap() {
	lbase.debug.Fine("Zapmap records:")
	for key, zrecs := range lbase.zmap.zapmap {
		var line string = ""
		for _, zrec := range zrecs {
			line += zrec.String()
		}
		lbase.debug.Fine("%q {%s}", key, line)
	}
}

func dumpLogfiles() {
	_, fnums, err := lbase.GetLogfilePaths()
	if err != nil {WrapError("Could not get logfile paths", err).Fatal()}
	for _, fnum := range fnums {
		lfile, err := lbase.GetLogfile(fnum)
		if err != nil {WrapError("Could not get logfile", err).Fatal()}
		lbase.debug.Fine("Logfile records for %s:", lfile.abspath)
		lrecs, err2 := lfile.Load()
		if err2 != nil {WrapError("Could not get logfile", err2).Fatal()}
		for _, lrec := range lrecs {
			lbase.debug.Fine(" %s", lrec.String())
		}
		//lbase.debug.Fine("Hex dump for %s:", lfile.abspath)
		//lfile.HexDump(0, 0)
	}
	return
}

// Put and get a key-value pair.
func saveRetrieveKeyValue(keystr, valstr string, t *testing.T) *Logbase {
	key := keystr
	val := []byte(valstr)

	_, err := lbase.Put(key, val, LBTYPE_STRING)
	if err != nil {
		t.Fatalf("Could not put key value pair into test logbase: %s", err)
	}

	got, vtype, err := lbase.Get(key)
	if err != nil {
		t.Fatalf("Could not get key value pair from test logbase: %s", err)
	}

	gotstr := string(got)
	vstr := string(val)
	if vstr != gotstr {
		t.Fatalf("The retrieved value %q differed from the expected value %q",
			gotstr, vstr)
	}

	if vtype != LBTYPE_STRING {
		t.Fatalf(
			"The retrieved value type %d differed from the expected value type %d",
			vtype, LBTYPE_STRING)
	}

	return lbase
}

func generateRandomKeyValuePairs(n, min, max uint64) (keys, values []string) {
	keys = GenerateRandomHexStrings(n, min, max)
	values = GenerateRandomHexStrings(n, min, max)
	return
}

func dumpKeyValuePairs(keys, values []string) {
	lbase.debug.Advise("Dumping key value pairs:")
	comlen := len(keys)
	if len(values) < len(keys) {comlen = len(values)}
	for i := 0; i < comlen; i++ {
		lbase.debug.Advise(" (%s,%s)", keys[i], values[i])
	}
	return
}

func LBUINTEqual(a, b []LBUINT) bool {
	if len(a) != len(b) {return false}
	for i, v := range a {
		if v != b[i] {return false}
	}
	return true
}
