/*
    Defines the data structures and their management.
*/
package logbase

import (
	"bufio"
	"bytes"
	"encoding/binary"
    "reflect"
    "hash/crc32"
    "fmt"
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

// Position of value in a log file.
type Vpos struct {
    vpos    LBUINT
}

// Provide a generic record file for "IO read infrastructure".
type GenericRecord struct {
    header  []byte
    *Ksize
    *Vsize
    *Kdata
    *Vdata
    *Vpos
}

// Define a log file record.
type LogRecord struct {
    crc     LBUINT // cyclic redundancy check
    *Ksize
    *Vsize
    *Kdata
    *Vdata
}

// Define an index file record.
type IndexRecord struct {
    *IndexRecordHeader
    *Kdata
}

// Define the header for an index file record.
type IndexRecordHeader struct {
    *Ksize
    *Vsize
    *Vpos
}

// Define a record used in the logbase master key index.
type MasterCatalogRecord struct {
    fnum    LBUINT // log files indexed sequentially from 0
    *Vsize
    *Vpos
}

// For in-memory use, initially replicates a master catalog record.
type ZapRecord struct {
    *MasterCatalogRecord
}

// Used by ReadRecord to read generic (packed) value size.
var GenericValueSize = map[int]LBUINT{
    LOG_RECORD:     0,
    INDEX_RECORD:   ValSizePosBytes(),
    MASTER_RECORD:  MasterRecordBytes(),
    ZAP_RECORD:     0,
}

func ValSizePosBytes() LBUINT {return LBUINT_SIZE_x2}
func MasterRecordBytes() LBUINT {return LBUINT_SIZE_x3}
func ZapRecordBytes() LBUINT {return MasterRecordBytes()}

// Data container methods.

func (mcr *MasterCatalogRecord) FromIndexRecord(irec *IndexRecord, fnum LBUINT) {
    mcr.fnum = fnum
    mcr.vsz = irec.vsz
    mcr.vpos = irec.vpos
}

func (zrec *ZapRecord) FromMasterCatalogRecord(mcr *MasterCatalogRecord) {
    zrec.fnum = mcr.fnum
    zrec.vsz = mcr.vsz
    zrec.vpos = mcr.vpos
}

// Map GenericRecord to a new LogRecord.
func (rec *GenericRecord) ToLogRecord() *LogRecord {
    lrec := new(LogRecord)
    lrec.ksz = rec.ksz
    lrec.vsz = rec.vsz - ParamSize(lrec.crc)
    lrec.key = rec.key
    // Unpack
	bfr := bufio.NewReader(bytes.NewBuffer(rec.val))
	binary.Read(bfr, binary.BigEndian, &lrec.val)
	binary.Read(bfr, binary.BigEndian, &lrec.crc)
    return lrec
}

// Map GenericRecord to a new IndexRecord.
func (rec *GenericRecord) ToIndexRecord() *IndexRecord {
    irec := new(IndexRecord)
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
    irec := new(IndexRecord)
    irec.ksz = lrec.ksz
    irec.vsz = lrec.vsz
    irec.key = lrec.key
    return irec
}

// Map GenericRecord to a new ZapRecord list.
func (rec *GenericRecord) ToZapRecordList() (string, []ZapRecord) {
    var keystr string = string(rec.key)
    n := rec.ZapLen()
	bfr := bufio.NewReader(bytes.NewBuffer(rec.val))
    var zrecs = make([]ZapRecord, n)
    for i := 0; i < n; i++ {
	    binary.Read(bfr, binary.BigEndian, &zrecs[i].fnum)
	    binary.Read(bfr, binary.BigEndian, &zrecs[i].vsz)
	    binary.Read(bfr, binary.BigEndian, &zrecs[i].vpos)
    }
    return keystr, zrecs
}

// Returns the number of ZapRecords in GenericRecord value, unless a partial
// zap record is detected, which is fatal.
func (rec *GenericRecord) ZapLen() int {
    zrecsize := ParamSize(new(ZapRecord))
    n := rec.vsz/zrecsize
    rem := rec.vsz - n * zrecsize
    if rem != 0 {FmtErrPartialZapData(zrecsize, rec.vsz).Fatal()}
    return int(n)
}

// Map GenericRecord to a new MasterLogRecord.
func (rec *GenericRecord) ToMasterCatalogRecord() *MasterCatalogRecord {
    mcr := new(MasterCatalogRecord)
    // Unpack
	bfr := bufio.NewReader(bytes.NewBuffer(rec.val))
	binary.Read(bfr, binary.BigEndian, &mcr.fnum)
	binary.Read(bfr, binary.BigEndian, &mcr.vsz)
	binary.Read(bfr, binary.BigEndian, &mcr.vpos)
    return mcr
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
    return lbase.OpenLogfile(mcr.fnum)
}

// Read the value pointed to by the master catalog record.
func (mcr *MasterCatalogRecord) ReadVal(lbase *Logbase) (val []byte, err error) {
    lfile, err := mcr.Logfile(lbase)
    if err != nil {return}
    return lfile.ReadVal(mcr.vpos, mcr.vsz)
}

// Byte packing functions.

func NewLogRecord(keystr string, val []byte) *LogRecord {
    lrec := new(LogRecord)
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
	binary.Write(bfr, binary.BigEndian, lrec.vsz + ParamSize(lrec.crc))
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
	binary.Write(bfr, binary.BigEndian, irec.vsz)
	binary.Write(bfr, binary.BigEndian, irec.vpos)
	bfr.Write(irec.key)
    return bfr.Bytes()
}

// Return a buffer with a zapmap record packed ready for file writing.
func PackZapRecord(keystr string, zrecs []ZapRecord) []byte {
	bfr := new(bytes.Buffer)
	key := []byte(keystr)
    ksz := AsLBUINT(len(key))
    n := AsLBUINT(len(zrecs))
	binary.Write(bfr, binary.BigEndian, ksz)
	binary.Write(bfr, binary.BigEndian, n)
	bfr.Write(key)
    for _, zrec := range zrecs {
	    binary.Write(bfr, binary.BigEndian, zrec.fnum)
	    binary.Write(bfr, binary.BigEndian, zrec.vsz)
	    binary.Write(bfr, binary.BigEndian, zrec.vpos)
    }
    return bfr.Bytes()
}

// Return a buffer with a master catalog record packed ready for file writing.
func PackMasterRecord(keystr string, mcr *MasterCatalogRecord) []byte {
	bfr := new(bytes.Buffer)
	binary.Write(bfr, binary.BigEndian, mcr.fnum)
	binary.Write(bfr, binary.BigEndian, mcr.vsz)
	binary.Write(bfr, binary.BigEndian, mcr.vpos)
    return bfr.Bytes()
}

func ParamSize(param interface{}) LBUINT {
    return LBUINT(reflect.TypeOf(param).Size())
}
