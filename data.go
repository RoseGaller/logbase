/*
	Defines the data structures and their management.
*/
package logbase

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"reflect"
	"hash/crc32"
	"fmt"
	"sort"
	"os"
)

const (
	LOG_RECORD = iota
	INDEX_RECORD = iota
	MASTER_RECORD = iota
	ZAP_RECORD = iota
)

// Whether to read a value size after the key size when using the
// GenericRecord to read these various record types.
var DoReadDataValueSize = map[int]bool{
	LOG_RECORD:     true,
	INDEX_RECORD:   false,
	MASTER_RECORD:  false,
	ZAP_RECORD:     true,
}

// Data containers.

// Key data container.
type Kdata struct {
	key     []byte
}

// Value data container.
type Vdata struct {
	val     []byte
}

// Key size in bytes.
type Ksize struct {
	ksz     LBUINT
}

// Value size in bytes.
type Vsize struct {
	vsz     LBUINT
}

// Position of value in file.
type Vpos struct {
	vpos    LBUINT
}

// Record size in bytes.
type Rsize struct {
	rsz     LBUINT
}

// Position of record in file.
type Rpos struct {
	rpos    LBUINT
}

// Provide a generic record file for "IO read infrastructure".
type GenericRecord struct {
	*Ksize
	*Vsize
	*Kdata
	*Vdata
	*Vpos
}

// Init a GenericRecord.
func NewGenericRecord() *GenericRecord {
	return &GenericRecord{
		Ksize: &Ksize{},
		Vsize: &Vsize{},
		Kdata: &Kdata{},
		Vdata: &Vdata{},
		Vpos: &Vpos{},
	}
}

// Define a log file record.
type LogRecord struct {
	crc     LBUINT // cyclic redundancy check
	*Ksize
	*Vsize
	*Kdata
	*Vdata
}

// Init a LogRecord.
func NewLogRecord() *LogRecord {
	return &LogRecord{
		Ksize: &Ksize{},
		Vsize: &Vsize{},
		Kdata: &Kdata{},
		Vdata: &Vdata{},
	}
}

// Define an index file record.
type IndexRecord struct {
	*IndexRecordHeader
	*Kdata
}

// Init a IndexRecord.
func NewIndexRecord() *IndexRecord {
	return &IndexRecord{
		IndexRecordHeader: NewIndexRecordHeader(),
		Kdata: &Kdata{},
	}
}

// Define the header for an index file record.
type IndexRecordHeader struct {
	*Ksize
	*Vsize
	*Vpos
}

// Init a IndexRecordHeader.
func NewIndexRecordHeader() *IndexRecordHeader {
	return &IndexRecordHeader{
		Ksize: &Ksize{},
		Vsize: &Vsize{},
		Vpos: &Vpos{},
	}
}

// Define the location and size of a value.
type ValueLocation struct {
	fnum    LBUINT // log files indexed sequentially from 0
	*Vsize
	*Vpos
}

// Init a ValueLocation.
func NewValueLocation() *ValueLocation {
	return &ValueLocation{
		Vsize: &Vsize{},
		Vpos: &Vpos{},
	}
}

// Define the location and size of a record.
type RecordLocation struct {
	fnum    LBUINT // log files indexed sequentially from 0
	*Rsize
	*Rpos
}

// Init a RecordLocation.
func NewRecordLocation() *RecordLocation {
	return &RecordLocation{
		Rsize: &Rsize{},
		Rpos: &Rpos{},
	}
}

// Define a record used in the logbase master key index.
type MasterCatalogRecord struct {
	*ValueLocation
}

// Init a MasterCatalogRecord, with flexibility to differentiate from a
// ValueLocationRecord.
func NewMasterCatalogRecord() *MasterCatalogRecord {
	return &MasterCatalogRecord{
		ValueLocation: NewValueLocation(),
	}
}

// Identify a logfile record for zapping.
type ZapRecord struct {
	*RecordLocation
}

// Init a ZapRecord.
func NewZapRecord() *ZapRecord {
	return &ZapRecord{
		RecordLocation: NewRecordLocation(),
	}
}

// Used by ReadRecord to read generic (packed) value size.
var GenericValueSize = map[int]LBUINT{
	LOG_RECORD:     0, // not used
	INDEX_RECORD:   ValSizePosBytes(),
	MASTER_RECORD:  ValueLocationBytes(),
	ZAP_RECORD:     0, // not used
}

func ValSizePosBytes() LBUINT {return LBUINT_SIZE_x2}
func ValueLocationBytes() LBUINT {return LBUINT_SIZE_x3}
func RecordLocationBytes() LBUINT {return LBUINT_SIZE_x3}

// Data container methods.

// Returns the number of ValueLocationRecords in GenericRecord value,
// unless a partial record is detected, which is fatal.
func (rec *GenericRecord) LocationListLength() int {
	vlocsize := ValueLocationBytes()
	n := rec.vsz/vlocsize
	rem := rec.vsz - n * vlocsize
	if rem != 0 {FmtErrPartialLocationData(vlocsize, rec.vsz).Fatal()}
	return int(n)
}

// Comparison.

// Compare for equality.
func (mcr *MasterCatalogRecord) Equals(other *MasterCatalogRecord) bool {
	return (mcr.fnum == other.fnum &&
		mcr.vsz == other.vsz &&
		mcr.vpos == other.vpos)
}

// Compare for equality.
func (zrec *ZapRecord) Equals(other *ZapRecord) bool {
	return (zrec.fnum == other.fnum &&
		zrec.rsz == other.rsz &&
		zrec.rpos == other.rpos)
}

// Interchanges.

func (vl *ValueLocation) FromIndexRecord(irec *IndexRecord, fnum LBUINT) {
	vl.fnum = fnum
	vl.vsz = irec.vsz
	vl.vpos = irec.vpos
}

func (zrec *ZapRecord) FromMasterCatalogRecord(keystr string, mcr *MasterCatalogRecord) {
	zrec.fnum = mcr.fnum
	rsz, rpos := mcr.LogRecordSizePosition(keystr)
	zrec.rsz = rsz
	zrec.rpos = rpos
}

// Map GenericRecord to a new LogRecord.
func (rec *GenericRecord) ToLogRecord() *LogRecord {
	lrec := NewLogRecord()
	lrec.ksz = rec.ksz
	lrec.vsz = rec.vsz - CRC_SIZE
	lrec.key = rec.key
	// Unpack
	bfr := bufio.NewReader(bytes.NewBuffer(rec.val))
	lrec.val = make([]byte, lrec.vsz) // must have fixed size
	binary.Read(bfr, binary.BigEndian, &lrec.val)
	binary.Read(bfr, binary.BigEndian, &lrec.crc)
	return lrec
}

// Map GenericRecord to a new IndexRecord.
func (rec *GenericRecord) ToIndexRecord() *IndexRecord {
	irec := NewIndexRecord()
	irec.ksz = rec.ksz
	irec.key = rec.key
	// Unpack
	bfr := bufio.NewReader(bytes.NewBuffer(rec.val))
	binary.Read(bfr, binary.BigEndian, &irec.vsz)
	binary.Read(bfr, binary.BigEndian, &irec.vpos)
	return irec
}

// Map LogRecord to an IndexRecord.  Note vpos is left as nil.
func (lrec *LogRecord) ToIndexRecord() *IndexRecord {
	irec := NewIndexRecord()
	irec.ksz = lrec.ksz
	irec.vsz = lrec.vsz
	irec.key = lrec.key
	return irec
}

// Map GenericRecord to a new ZapRecord list.
func (rec *GenericRecord) ToZapRecordList() (string, []*ZapRecord) {
	var keystr string = string(rec.key)
	n := rec.LocationListLength()
	bfr := bufio.NewReader(bytes.NewBuffer(rec.val))
	var zrecs = make([]*ZapRecord, n)
	for i := 0; i < n; i++ {
		zrecs[i] = NewZapRecord()
	    binary.Read(bfr, binary.BigEndian, &zrecs[i].fnum)
	    binary.Read(bfr, binary.BigEndian, &zrecs[i].rsz)
	    binary.Read(bfr, binary.BigEndian, &zrecs[i].rpos)
	}
	return keystr, zrecs
}
// Map GenericRecord to a new MasterLogRecord.
func (rec *GenericRecord) ToMasterCatalogRecord() *MasterCatalogRecord {
	mcr := NewMasterCatalogRecord()
	// Unpack
	bfr := bufio.NewReader(bytes.NewBuffer(rec.val))
	binary.Read(bfr, binary.BigEndian, &mcr.fnum)
	binary.Read(bfr, binary.BigEndian, &mcr.vsz)
	binary.Read(bfr, binary.BigEndian, &mcr.vpos)
	return mcr
}

// Formatted output.

// Return string representation of a GenericRecord for debugging.
func (rec *GenericRecord) String() string {
	return fmt.Sprintf(
		"(ksz %d, vsz %d, key %q, val %q, vpos %d)",
		rec.ksz,
		rec.vsz,
		string(rec.key),
		string(rec.val),
		rec.vpos)
}

// Return string representation of a GenericRecord for debugging.
func (lrec *LogRecord) String() string {
	return fmt.Sprintf(
		"(%d,%d,%q,%q,%d)",
		lrec.ksz,
		lrec.vsz,
		string(lrec.key),
		string(lrec.val),
		lrec.crc)
}

// Return string representation of an IndexRecord.
func (irec *IndexRecord) String() string {
	return fmt.Sprintf(
		"(%d,%d,%d,%q)",
		irec.ksz,
		irec.vpos,
		irec.vsz,
		string(irec.key))
}

// Return string representation of a MasterCatalogRecord.
func (mcr *MasterCatalogRecord) String() string {
	return fmt.Sprintf(
		"(%d,%d,%d)",
		mcr.fnum,
		mcr.vpos,
		mcr.vsz)
}

// Return string representation of a ZapRecord.
func (zrec *ZapRecord) String() string {
	return fmt.Sprintf(
		"(%d,%d,%d)",
		zrec.fnum,
		zrec.rpos,
		zrec.rsz)
}

// LBUINT related functions.

func (i LBUINT) plus(other int) LBUINT {
	ans := int(i) + other
	return AsLBUINT(ans)
}

func AsLBUINT(num int) LBUINT {
	if !CanMakeLBUINT(int64(num)) {
		msg := fmt.Sprintf(
			"The number %d does not fit within " +
			"[0, LBUINT_MAX] = [0, %d]",
			num, LBUINT_MAX)
		ErrIntMismatch(msg).Fatal()
	}
	return LBUINT(num)
}

// Can the given int be cast to LBUINT without overflow?
func CanMakeLBUINT(num int64) bool {
	if num <= LBUINT_MAX && num >= 0 {return true}
	return false
}

// File related methods for data containers.

// Return the logfile pointed to by the master catalog record.
func (mcr *MasterCatalogRecord) Logfile(lbase *Logbase) (*Logfile, error) {
	return lbase.GetLogfile(mcr.fnum)
}

// Read the value pointed to by the master catalog record.
func (mcr *MasterCatalogRecord) ReadVal(lbase *Logbase) (val []byte, err error) {
	lfile, err := mcr.Logfile(lbase)
	if err != nil {return}
	return lfile.ReadVal(mcr.vpos, mcr.vsz)
}

// Byte packing functions.

func MakeLogRecord(keystr string, val []byte) *LogRecord {
	lrec := NewLogRecord()
	lrec.key = []byte(keystr)
	lrec.ksz = AsLBUINT(len(lrec.key))
	lrec.val = val
	lrec.vsz = AsLBUINT(len(val))
	return lrec
}

// Return a buffer with a log record packed ready for file writing.
func (lrec *LogRecord) Pack() []byte {
	bfr := new(bytes.Buffer)
	binary.Write(bfr, binary.BigEndian, lrec.ksz)
	binary.Write(bfr, binary.BigEndian, lrec.vsz + CRC_SIZE)
	bfr.Write(lrec.key)
	bfr.Write(lrec.val)

	// Calculate the checksum
	lrec.crc = LBUINT(crc32.ChecksumIEEE(bfr.Bytes()))
	binary.Write(bfr, binary.BigEndian, lrec.crc)
	return bfr.Bytes()
}

// Return a buffer with a log file index record packed ready for file writing.
func (irec *IndexRecord) Pack() []byte {
	bfr := new(bytes.Buffer)
	binary.Write(bfr, binary.BigEndian, irec.ksz)
	bfr.Write(irec.key)
	binary.Write(bfr, binary.BigEndian, irec.vsz)
	binary.Write(bfr, binary.BigEndian, irec.vpos)
	return bfr.Bytes()
}

// Return a buffer with a zapmap record packed ready for file writing.
func PackZapRecord(keystr string, zrecs []*ZapRecord) []byte {
	bfr := new(bytes.Buffer)
	key := []byte(keystr)
	ksz := AsLBUINT(len(key))
	n := AsLBUINT(len(zrecs)) * ValueLocationBytes()
	binary.Write(bfr, binary.BigEndian, ksz)
	binary.Write(bfr, binary.BigEndian, n)
	bfr.Write(key)
	for _, zrec := range zrecs {
	    binary.Write(bfr, binary.BigEndian, zrec.fnum)
	    binary.Write(bfr, binary.BigEndian, zrec.rsz)
	    binary.Write(bfr, binary.BigEndian, zrec.rpos)
	}
	return bfr.Bytes()
}

// Return a buffer with a master catalog record packed ready for file writing.
func PackMasterRecord(keystr string, mcr *MasterCatalogRecord) []byte {
	bfr := new(bytes.Buffer)
	key := []byte(keystr)
	ksz := AsLBUINT(len(key))
	binary.Write(bfr, binary.BigEndian, ksz)
	bfr.Write(key)
	binary.Write(bfr, binary.BigEndian, mcr.fnum)
	binary.Write(bfr, binary.BigEndian, mcr.vsz)
	binary.Write(bfr, binary.BigEndian, mcr.vpos)
	return bfr.Bytes()
}

// Size calculations.

// Use reflection to find the byte size of any parameter, as an LBUINT.
func ParamSize(param interface{}) LBUINT {
	return LBUINT(reflect.TypeOf(param).Size())
}

// MasterCatalogRecords do not explicitely hold the start position and length
// of an entire logfile record, just the value, but along with the key we have
// enough to figure this out.
func (mcr *MasterCatalogRecord) LogRecordSizePosition(keystr string) (size, pos LBUINT) {
	ksz := AsLBUINT(len(keystr))
	size = LBUINT_SIZE_x2 + ksz + mcr.vsz + CRC_SIZE
	pos = mcr.vpos - ksz - LBUINT_SIZE_x2
	return
}

// Zapping.

// Return the number of whole buffers that divide the chunk, and any remainder.
func DivideChunkByBuffer(chunksize, buffersize LBUINT) (n, rem LBUINT) {
	n = chunksize / buffersize
	rem = chunksize - n * buffersize
	return
}

// Invert the zap lengths to preserved lengths (chunks) in a data sequence.
func InvertSequence(zpos, zsz []LBUINT, size int) (pos, sz []LBUINT) {
	if len(zpos) != len(zsz) {
		ErrNew(fmt.Sprintf(
			"Zap position and size slice lengths differ, " +
			"being %d and %d respectively.",
			len(zpos), len(zsz))).Fatal()
	}
	var p []LBUINT
	for i, zp := range zpos {
		p = append(p, zp)
		p = append(p, zp + zsz[i])
	}
	s := AsLBUINT(size)
	var j int = 1
	if p[0] != 0 {
		p = append([]LBUINT{0}, p...)
		j = 0
	}
	if p[len(p) - 1] != s {p = append(p, s)}
	p = RemoveAdjacentDuplicates(p)

	for ; j < len(p) - 1; j = j + 2 {
		pos = append(pos, p[j])
		sz = append(sz, p[j+1] - p[j])
	}
	return
}

// Remove adjacent duplicates from the given slice.
func RemoveAdjacentDuplicates(a []LBUINT) (b []LBUINT) {
	var idups []int
	for i := 1; i < len(a); i++ { // note start from 1
		if a[i] == a[i-1] {idups = append(idups, []int{i-1,i}...)}
	}
	var include bool
	for i := 0; i < len(a); i++ {
		include = true
		for _, j := range idups {if i == j {include = false}}
		if include {b = append(b, a[i])}
	}
	return
}

// Return "zaplists" for the given logfile number, that is, a slice each for
// start positions and lengths that must be zapped from the file.  Adjacent
// lengths are merged, and the results are sorted by position.
func (zmap *Zapmap) Find(fnum LBUINT) (rpos, rsz []LBUINT, err error) {
	sz := make(map[int]LBUINT)
	var rposi []int // Allows us to sort the size map by rpos using int
	for _, zrecs := range zmap.zapmap {
		for _, zrec := range zrecs {
			if zrec.fnum == fnum {
				_, exists := sz[int(zrec.rpos)]
				if exists {
					err = FmtErrKeyExists(string(zrec.rpos))
					return
				}
				sz[int(zrec.rpos)] = zrec.rsz
				rposi = append(rposi, int(zrec.rpos))
			}
		}
	}

	// Sort position and size of data to zap
	sort.Ints(rposi)
	rpos = make([]LBUINT, len(rposi))
	rsz = make([]LBUINT, len(rposi))
	for i, pos := range rposi {
		rpos[i] = LBUINT(pos)
		rsz[i] = sz[pos]
	}

	return
}

// Delete all zapmap records associated with the given logfile number.
func (zmap *Zapmap) Purge(fnum LBUINT, debug *DebugLogger) {
	debug.Basic(DEBUG_DEFAULT, "Purge zapmap of logfile %d entries", fnum)
	for key, zrecs := range zmap.zapmap {
		var newzrecs []*ZapRecord // Make a new list to replace old
		for _, zrec := range zrecs {
			if zrec.fnum != fnum {
				newzrecs = append(newzrecs, zrec)
			} else {
				debug.Fine(
					DEBUG_DEFAULT,
					"Deleting %q%s from zapmap",
					key, zrec.String())
			}
		}
		if len(newzrecs) == 0 {
			delete(zmap.zapmap, key)
		} else {
			zmap.zapmap[key] = newzrecs
		}
	}
	return
}

// Random numbers.


// Generate a slice of random hex strings of random length within the given
// range of lengths.
// Credit to Russ Cox https://groups.google.com/forum/#!topic/golang-nuts/d0nF_k4dSx4
// for the idea of using /dev/urandom.
// TODO check cross compatibility of /dev/urandom
func GenerateRandomHexStrings(n, minsize, maxsize uint64) (result []string) {
	frnd, _ := os.OpenFile("/dev/urandom", os.O_RDONLY, 0)
	defer frnd.Close()

	maxuint := float64(^uint64(0))
	rng := float64(maxsize - minsize)
	if rng < 0 {
		ErrNew(fmt.Sprintf("maxsize %d must be >= minsize %d", maxsize, minsize)).Fatal()
	}
	var adjlen, rawlen uint64
	result = make([]string, n)
	for i := 0; i < int(n); i++ {
		binary.Read(frnd, binary.BigEndian, &rawlen)
		adjlen = uint64(float64(rawlen)*rng/maxuint) + minsize
		rndval := make([]byte, int(adjlen)/2)
		frnd.Read(rndval)
		result[i] = hex.EncodeToString(rndval)
	}
	return
}
