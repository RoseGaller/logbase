/*
	Defines the data structures and their management.
*/
package logbase

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"reflect"
	"hash/crc32"
	"fmt"
	"sort"
	"sync"
)

var BIGEND binary.ByteOrder = binary.BigEndian

const (
	LOG_RECORD int = iota
	INDEX_RECORD
	MASTER_RECORD
	ZAP_RECORD
	PERMISSION_RECORD
)

type FileDecodeConfig struct {
	readDataValueSize	bool // Read a value size (GVS) after the key size (KS)?
	snipValueType		bool // Snip LBTYPE value from the returned value bytes?
	genericValueSize	LBUINT
}

var FileDecodeConfigs = map[int]*FileDecodeConfig{
	LOG_RECORD:			&FileDecodeConfig{true,		true,	0},
	INDEX_RECORD:		&FileDecodeConfig{false,	false,	LBUINT_SIZE_x2},
	MASTER_RECORD:		&FileDecodeConfig{false,	false,	ValueLocationBytes()},
	ZAP_RECORD:			&FileDecodeConfig{true,		false,	0},
	PERMISSION_RECORD:	&FileDecodeConfig{false,	false,	LBUINT(1)},
}

// Data containers.

// Key data container.
type Kdata struct {
	kbyts   []byte
}

// Key type.
type Ktype struct {
	ktype   LBTYPE
}

// Value data container.
type Vdata struct {
	vbyts   []byte
}

// Value type.
type Vtype struct {
	vtype   LBTYPE
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
	*Ksize  // typed key, including LBTYPE
	*Vsize  // typed value, including LBTYPE
	*Kdata
	*Ktype
	*Vdata  // type snipped only according to FileDecodeConfgs
	*Vtype
	*Vpos
}

// Init a GenericRecord.
func NewGenericRecord() *GenericRecord {
	return &GenericRecord{
		Ksize: &Ksize{},
		Vsize: &Vsize{},
		Kdata: &Kdata{},
		Ktype: &Ktype{},
		Vdata: &Vdata{},
		Vtype: &Vtype{},
		Vpos: &Vpos{},
	}
}

// Define a log file record.
type LogRecord struct {
	crc     LBUINT // cyclic redundancy check
	*Ksize  // typed key, including LBTYPE
	*Vsize	// typed value, including LBTYPE
	*Kdata
	*Ktype
	*Vdata  // does not include LBTYPE
	*Vtype
}

// Init a LogRecord.
func NewLogRecord() *LogRecord {
	return &LogRecord{
		Ksize: &Ksize{},
		Vsize: &Vsize{},
		Kdata: &Kdata{},
		Ktype: &Ktype{},
		Vdata: &Vdata{},
		Vtype: &Vtype{},
	}
}

// Define an index file record.
type IndexRecord struct {
	*IndexRecordHeader
	*Kdata
	*Ktype
}

// Init a IndexRecord.
func NewIndexRecord() *IndexRecord {
	return &IndexRecord{
		IndexRecordHeader: NewIndexRecordHeader(),
		Kdata: &Kdata{},
		Ktype: &Ktype{},
	}
}

// Define the header for an index file record.
type IndexRecordHeader struct {
	*Ksize  // typed key, that is including LBTYPE  
	*Vsize  // typed value, that is including LBTYPE
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
	*Vsize	// typed value, that is including LBTYPE
	*Vpos
}

// Init a ValueLocation.
func NewValueLocation() *ValueLocation {
	return &ValueLocation{
		Vsize: &Vsize{},
		Vpos: &Vpos{},
	}
}

// Define a data container.
type Value struct {
	vtype	LBTYPE
	*Vdata  // Data with LBTYPE snipped off the front
	*ValueLocation
}

// Init a Value.
func NewValue() *Value {
	return &Value{
		vtype: LBTYPE_NIL,
		Vdata: &Vdata{},
		ValueLocation: NewValueLocation(),
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
// Current implementors:
//  Value
//  ValueLocation
type MasterCatalogRecord interface {
	Equals(other MasterCatalogRecord) bool
	String() string
	ToValueLocation() *ValueLocation
	ReadVal(lbase *Logbase) (val []byte, vtype LBTYPE, err error)
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

// Define a record used in the user permission index.
type UserPermissionRecord struct {
	*Permission
}

// Init a UserPermissionRecord.
func NewUserPermissionRecord() *UserPermissionRecord {
	return &UserPermissionRecord{
		Permission: new(Permission),
	}
}

func ValueLocationBytes() LBUINT {return VALOC_SIZE}

// Logbase level.

//  Index of all key-value pairs in a log file.
type Index struct {
	list    []*IndexRecord
}

//  Master catalog of all live (not stale) key-value pairs.
type MasterCatalog struct {
	index   map[interface{}]MasterCatalogRecord // The in-memory index
	file    *Masterfile
	sync.RWMutex
	changed	bool // Has index changed since last save?
}

// Init a MasterCatalog.
func NewMasterCatalog() *MasterCatalog {
	return &MasterCatalog{
		index: make(map[interface{}]MasterCatalogRecord),
	}
}

//  Stale key-value pairs scheduled to be deleted from log files.
type Zapmap struct {
	zapmap  map[interface{}][]*ZapRecord // "Zapmap"
	file    *Zapfile
	sync.RWMutex
	changed	bool // Has map changed since last save?
}

// Init a Zapmap, which points to stale data scheduled for deletion.
func NewZapmap() *Zapmap {
	return &Zapmap{
		zapmap: make(map[interface{}][]*ZapRecord),
	}
}

// All users of the logbase.
type Users struct {
	perm	map[string]*UserPermissions
}

// Init a Users object.
func NewUsers() *Users {
	return &Users{
		perm: make(map[string]*UserPermissions),
	}
}

//  User permission index allowing k-v pair level security.
type UserPermissions struct {
	index   map[interface{}]*UserPermissionRecord // The in-memory index
	file    *UserPermissionFile
	sync.RWMutex
	pass	string
	changed	bool // Has index changed since last save?
}

// Init a UserPermissions object.
func NewUserPermissions(p *Permission) *UserPermissions {
	up := &UserPermissions{
		index: make(map[interface{}]*UserPermissionRecord),
	}
	up.Put(uint8(0), &UserPermissionRecord{p}) // Default
	return up
}

// Logbase methods.

// Update the Zapmap.
func (lbase *Logbase) UpdateZapmap(irec *IndexRecord, fnum LBUINT) (interface{}, *ValueLocation) {
	newvloc := NewValueLocation()
	newvloc.FromIndexRecord(irec, fnum)
	key, err := MakeKey(irec.kbyts, irec.ktype, lbase.debug)
	lbase.debug.Error(err)
	old := lbase.mcat.Get(key)

	if old != nil {
		vloc := old.ToValueLocation()
		// Add to zapmap
		zrec := NewZapRecord()
		rloc := vloc.ToRecordLocation(irec.ksz)
		zrec.RecordLocation = rloc
		lbase.zmap.PutRecord(key, zrec)
	}

	return key, newvloc
}

// Update the Master Catalog.
func (lbase *Logbase) UpdateMasterCatalog(key interface{}, mcr MasterCatalogRecord) MasterCatalogRecord {
	lbase.mcat.Put(key, mcr)
	SetNextMCID(key)
	return mcr
}

// Gateway for reading from master catalog.
func (mcat *MasterCatalog) Get(key interface{}) MasterCatalogRecord {
	mcat.RLock() // other reads ok
	mcr := mcat.index[key]
	mcat.RUnlock()
	return mcr
}

// Gateway for writing to master catalog.
func (mcat *MasterCatalog) Put(key interface{}, mcr MasterCatalogRecord) {
	mcat.Lock()
	mcat.index[key] = mcr
	mcat.Unlock()
	mcat.changed = true
	return
}

// Gateway for removing entkey,ire entry from master catalog.
func (mcat *MasterCatalog) Delete(key interface{}) {
	mcat.Lock()
	delete(mcat.index, key)
	mcat.Unlock()
	mcat.changed = true
	return
}

// Gateway for reading from zapmap.
func (zmap *Zapmap) Get(key interface{}) []*ZapRecord {
	zmap.RLock() // other reads ok
	zrecs := zmap.zapmap[key]
	zmap.RUnlock()
	return zrecs
}

// Gateway for adding single record into to zapmap.
func (zmap *Zapmap) PutRecord(key interface{}, zrec *ZapRecord) {
	zrecs := zmap.Get(key)
	zrecs = append(zrecs, zrec)
	zmap.Put(key, zrecs)
	return
}

// Gateway for adding entire record slice into to zapmap.
func (zmap *Zapmap) Put(key interface{}, zrecs []*ZapRecord) {
	zmap.Lock()
	zmap.zapmap[key] = zrecs
	zmap.Unlock()
	zmap.changed = true
	return
}

// Gateway for removing entire entry from zapmap.
func (zmap *Zapmap) Delete(key interface{}) {
	zmap.Lock()
	delete(zmap.zapmap, key)
	zmap.Unlock()
	zmap.changed = true
	return
}

// Gateway for reading from user permission index.
func (up *UserPermissions) Get(key interface{}) *UserPermissionRecord {
	up.RLock() // other reads ok
	upr := up.index[key]
	up.RUnlock()
	return upr
}

// Gateway for writing to user permission index.
func (up *UserPermissions) Put(key interface{}, upr *UserPermissionRecord) {
	up.Lock()
	up.index[key] = upr
	up.Unlock()
	up.changed = true
	return
}

// Gateway for removing entire entry from user permission index.
func (up *UserPermissions) Delete(key interface{}) {
	up.Lock()
	delete(up.index, key)
	up.Unlock()
	up.changed = true
	return
}

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

// Compare for equality against MasterCatalogRecord interface.
func (vloc *ValueLocation) Equals(other MasterCatalogRecord) bool {
	if other == nil {return false}
	if othervloc, ok := other.(*ValueLocation); ok {
		return (vloc.fnum == othervloc.fnum &&
			vloc.vsz == othervloc.vsz &&
			vloc.vpos == othervloc.vpos)
	}
	return false
}

// Compare for equality against MasterCatalogRecord interface.
func (val *Value) Equals(other MasterCatalogRecord) bool {
	if other == nil {return false}
	return val.ValueLocation.Equals(other.ToValueLocation())
}

// Compare for equality against another ZapRecord.
func (zrec *ZapRecord) Equals(other *ZapRecord) bool {
	if other == nil {return false}
	return (zrec.fnum == other.fnum &&
		zrec.rsz == other.rsz &&
		zrec.rpos == other.rpos)
}

// Unpacking and interchanges.

// Inject type byte(s) at start of key or value byte slice.
func InjectType(x []byte, typ LBTYPE) []byte {
	bfr := new(bytes.Buffer)
	binary.Write(bfr, BIGEND, typ)
	bfr.Write(x)
	return bfr.Bytes()
}

// Read the first bytes of the given byte slice to return the LBTYPE.
func GetType(x []byte, debug *DebugLogger) (typ LBTYPE) {
	if len(x) < LBTYPE_SIZE {
		debug.Error(FmtErrSliceTooSmall(x, LBTYPE_SIZE))
	}
	bfr := bufio.NewReader(bytes.NewBuffer(x[:LBTYPE_SIZE]))
	debug.DecodeError(binary.Read(bfr, BIGEND, &typ))
	return
}

// Convert the given key to a byte representation.  Handles either
// Go numbers or strings.
func KeyToBytes(key interface{}) []byte {
	bfr := new(bytes.Buffer)
	str, ok := key.(string)
	if ok {key = []byte(str)}
	binary.Write(bfr, BIGEND, key)
	return bfr.Bytes()
}

// Inject the key type into the byte representation of the key, in
// one step.
func InjectKeyType(key interface{}, debug *DebugLogger) []byte {
	kbyts := KeyToBytes(key)
	ktype := GetKeyType(key, debug)
	return InjectType(kbyts, ktype)
}

func SnipValueType(val []byte, debug *DebugLogger) (newval []byte, vtype LBTYPE) {
	vtype = GetType(val, debug)
	newval = val[LBTYPE_SIZE:]
	return
}

func SnipKeyType(key []byte, debug *DebugLogger)  (newkey []byte, ktype LBTYPE) {
	bfr := bufio.NewReader(bytes.NewBuffer(key[:LBTYPE_SIZE]))
	debug.DecodeError(binary.Read(bfr, BIGEND, &ktype))
	newkey	= key[LBTYPE_SIZE:]
	return
}

func (vloc *ValueLocation) FromIndexRecord(irec *IndexRecord, fnum LBUINT) {
	vloc.fnum = fnum
	vloc.vsz = irec.vsz
	vloc.vpos = irec.vpos
	return
}

func (zrec *ZapRecord) FromValueLocation(ksz LBUINT, vloc *ValueLocation) {
	zrec.fnum = vloc.fnum
	rloc := vloc.ToRecordLocation(ksz)
	zrec.rsz = rloc.rsz
	zrec.rpos = rloc.rpos
	return
}

// Map GenericRecord to a new LogRecord.
func (rec *GenericRecord) ToLogRecord(debug *DebugLogger) *LogRecord {
	lrec := NewLogRecord()
	lrec.ksz = rec.ksz
	lrec.vsz = rec.vsz - CRC_SIZE
	lrec.kbyts = rec.kbyts
	lrec.ktype = rec.ktype
	lrec.vtype = rec.vtype
	// Unpack
	bfr := bufio.NewReader(bytes.NewBuffer(rec.vbyts))
	// Note that the generic vsz includes the LBTYPE prefix
	lrec.vbyts = make([]byte, int(lrec.vsz) - LBTYPE_SIZE) // must have fixed size
	debug.DecodeError(binary.Read(bfr, BIGEND, &lrec.vbyts))
	debug.DecodeError(binary.Read(bfr, BIGEND, &lrec.crc))
	return lrec
}

// Map GenericRecord to a new IndexRecord.
func (rec *GenericRecord) ToIndexRecord(debug *DebugLogger) *IndexRecord {
	irec := NewIndexRecord()
	irec.ksz = rec.ksz
	irec.kbyts = rec.kbyts
	irec.ktype = rec.ktype
	// Unpack
	bfr := bufio.NewReader(bytes.NewBuffer(rec.vbyts))
	debug.DecodeError(binary.Read(bfr, BIGEND, &irec.vsz))
	debug.DecodeError(binary.Read(bfr, BIGEND, &irec.vpos))
	return irec
}

// Map LogRecord to an IndexRecord.  Note vpos is left as nil.
func (lrec *LogRecord) ToIndexRecord(debug *DebugLogger) *IndexRecord {
	irec := NewIndexRecord()
	irec.ksz = lrec.ksz
	irec.vsz = lrec.vsz
	irec.kbyts = lrec.kbyts
	irec.ktype = lrec.ktype
	return irec
}

// Map GenericRecord to a new MasterLogRecord.
func (rec *GenericRecord) ToValueLocation(debug *DebugLogger) (interface{}, *ValueLocation) {
	key, err := MakeKey(rec.kbyts, rec.ktype, debug)
	debug.Error(err)
	vbyts, _ := rec.GetValueAndType(MASTER_RECORD, debug)
	vloc := NewValueLocation()
	// Unpack
	bfr := bufio.NewReader(bytes.NewBuffer(vbyts))
	debug.DecodeError(binary.Read(bfr, BIGEND, &vloc.fnum))
	debug.DecodeError(binary.Read(bfr, BIGEND, &vloc.vsz))
	debug.DecodeError(binary.Read(bfr, BIGEND, &vloc.vpos))
	return key, vloc
}

// Map GenericRecord to a new ZapRecord list.
func (rec *GenericRecord) ToZapRecordList(debug *DebugLogger) (interface{}, []*ZapRecord) {
	key, err := MakeKey(rec.kbyts, rec.ktype, debug)
	debug.Error(err)
	n := rec.LocationListLength()
	bfr := bufio.NewReader(bytes.NewBuffer(rec.vbyts))
	var zrecs = make([]*ZapRecord, n)
	for i := 0; i < n; i++ {
		zrecs[i] = NewZapRecord()
	    debug.DecodeError(binary.Read(bfr, BIGEND, &zrecs[i].fnum))
	    debug.DecodeError(binary.Read(bfr, BIGEND, &zrecs[i].rsz))
	    debug.DecodeError(binary.Read(bfr, BIGEND, &zrecs[i].rpos))
	}
	return key, zrecs
}

// Map GenericRecord to a new UserPermissionRecord.
func (rec *GenericRecord) ToUserPermissionRecord(debug *DebugLogger) (interface{}, *UserPermissionRecord) {
	key, err := MakeKey(rec.kbyts, rec.ktype, debug)
	debug.Error(err)
	upr := NewUserPermissionRecord()
	// Unpack
	var p uint8 = uint8(rec.vbyts[0])
	upr.Create = PERMISSION_CREATE == (p & PERMISSION_CREATE)
	upr.Read = PERMISSION_READ == (p & PERMISSION_READ)
	upr.Update = PERMISSION_UPDATE == (p & PERMISSION_UPDATE)
	upr.Delete = PERMISSION_DELETE == (p & PERMISSION_DELETE)
	return key, upr
}

// Extract ValueLocation from itself (identity).
func (vloc *ValueLocation) ToValueLocation() *ValueLocation {
	return vloc
}

// Extract ValueLocation from Value.
func (val *Value) ToValueLocation() *ValueLocation {
	return val.ValueLocation
}

func (rec *GenericRecord) GetValueAndType(rectype int, debug *DebugLogger) ([]byte, LBTYPE) {
	if FileDecodeConfigs[rectype].snipValueType {
		// LBTYPE has already been snipped
		return rec.vbyts, rec.vtype
	} else {
        return SnipValueType(rec.vbyts, debug)
	}
}

// Formatted output for debugging.

// Return string representation of a GenericRecord for debugging.
func (rec *GenericRecord) String() string {
	return fmt.Sprintf(
		"(ksz=%d vsz=%d kbyts=%v key=%q ktype=%d val=%v vtype=%d vpos=%d)",
		rec.ksz,
		rec.vsz,
		rec.kbyts,
		string(rec.kbyts),
		rec.ktype,
		rec.vbyts,
		rec.vtype,
		rec.vpos)
}

// Return string representation of a GenericRecord for debugging.
func (lrec *LogRecord) String() string {
	return fmt.Sprintf(
		"(ksz=%d vsz=%d key=%q ktype=%d val=%q vtype=%d crc=%d)",
		lrec.ksz,
		lrec.vsz,
		string(lrec.kbyts),
		lrec.ktype,
		string(lrec.vbyts),
		lrec.vtype,
		lrec.crc)
}

// Return string representation of an IndexRecord.
func (irec *IndexRecord) String() string {
	return fmt.Sprintf(
		"(ksz=%d vpos=%d vsz=%d key=%q ktype=%d)",
		irec.ksz,
		irec.vpos,
		irec.vsz,
		string(irec.kbyts),
		irec.ktype)
}

// Return string representation of a ValueLocation.
func (vloc *ValueLocation) String() string {
	return fmt.Sprintf(
		"(fnum=%d vsz=%d vpos=%d)",
		vloc.fnum,
		vloc.vsz,
		vloc.vpos)
}

// Return string representation of a Value.
func (val *Value) String() string {
	return fmt.Sprintf(
		"(vtype=%d val=%s %s)",
		val.vtype,
		ValBytesToString(val.vbyts, val.vtype),
		val.ValueLocation.String())
}

// Return string representation of a ZapRecord.
func (zrec *ZapRecord) String() string {
	return fmt.Sprintf(
		"(fnum=%d rsz=%d rpos=%d)",
		zrec.fnum,
		zrec.rsz,
		zrec.rpos)
}

// LBUINT related functions.

func (i LBUINT) Plus(other int) LBUINT {
	ans := int(i) + other
	return AsLBUINT(ans)
}

func AsLBUINT(num int) LBUINT {
	if !CanMakeLBUINT(int64(num)) {FmtErrOutsideRange(num, LBUINT_MAX).Fatal()}
	return LBUINT(num)
}

// Can the given int be cast to LBUINT without overflow?
func CanMakeLBUINT(num int64) bool {
	if num <= LBUINT_MAX && num >= 0 {return true}
	return false
}

// File related methods for data containers.

// Return the logfile pointed to by the master catalog record.
func (vloc *ValueLocation) Logfile(lbase *Logbase) (*Logfile, error) {
	return lbase.GetLogfile(vloc.fnum)
}

// Read the value pointed to by the ValueLocation and snip off the
// leading LBTYPE.
func (vloc *ValueLocation) ReadVal(lbase *Logbase) (val []byte, vtype LBTYPE, err error) {
	lfile, err := vloc.Logfile(lbase)
	if err != nil {return}
	vbyts, err := lfile.ReadVal(vloc.vpos, vloc.vsz)
	val, vtype = SnipValueType(vbyts, lbase.debug)
	return
}

// Read the value pointed to by the Value.
func (val *Value) ReadVal(lbase *Logbase) ([]byte, LBTYPE, error) {
	return val.vbyts, val.vtype, nil
}

// Byte packing functions.

func MakeLogRecord(key interface{}, val []byte, vtype LBTYPE, debug *DebugLogger) *LogRecord {
	lrec := NewLogRecord()
	ktype := GetKeyType(key, debug)
	kbyts := KeyToBytes(key)
	lrec.kbyts = kbyts
	lrec.ktype = ktype
	lrec.ksz = AsLBUINT(len(kbyts) + LBTYPE_SIZE)
	lrec.vbyts = val
	lrec.vtype = vtype
	lrec.vsz = AsLBUINT(len(val) + LBTYPE_SIZE)
	return lrec
}

// Return a byte slice with a log record packed ready for file writing.
func (lrec *LogRecord) Pack() []byte {
	bfr := new(bytes.Buffer)
	binary.Write(bfr, BIGEND, lrec.ksz)
	binary.Write(bfr, BIGEND, lrec.vsz + CRC_SIZE)
	bfr.Write(InjectType(lrec.kbyts, lrec.ktype))
	bfr.Write(InjectType(lrec.vbyts, lrec.vtype))

	// Calculate the checksum
	lrec.crc = LBUINT(crc32.ChecksumIEEE(bfr.Bytes()))
	binary.Write(bfr, BIGEND, lrec.crc)
	return bfr.Bytes()
}

// Return a byte slice with a log file index record packed ready for file
// writing.
func (irec *IndexRecord) Pack() []byte {
	bfr := new(bytes.Buffer)
	binary.Write(bfr, BIGEND, irec.ksz)
	bfr.Write(InjectType(irec.kbyts, irec.ktype))
	binary.Write(bfr, BIGEND, irec.vsz)
	binary.Write(bfr, BIGEND, irec.vpos)
	return bfr.Bytes()
}

// Return a byte slice with a zapmap record packed ready for file writing.
func PackZapRecord(key interface{}, zrecs []*ZapRecord, debug *DebugLogger) []byte {
	bfr := new(bytes.Buffer)
	kbyts := InjectKeyType(key, debug)
	ksz := AsLBUINT(len(kbyts))
	vsz := AsLBUINT(len(zrecs)) * ValueLocationBytes()
	binary.Write(bfr, BIGEND, ksz)
	binary.Write(bfr, BIGEND, vsz)
	bfr.Write(kbyts)
	for _, zrec := range zrecs {
	    binary.Write(bfr, BIGEND, zrec.fnum)
	    binary.Write(bfr, BIGEND, zrec.rsz)
	    binary.Write(bfr, BIGEND, zrec.rpos)
	}
	return bfr.Bytes()
}

// Return a byte slice with a ValueLocation packed ready for file writing.
func (vloc *ValueLocation) Pack(key interface{}, debug *DebugLogger) []byte {
	bfr := new(bytes.Buffer)
	byts := PackKey(key, debug)
	bfr.Write(byts)
	binary.Write(bfr, BIGEND, LBTYPE_VALOC)
	binary.Write(bfr, BIGEND, vloc.fnum)
	binary.Write(bfr, BIGEND, vloc.vsz)
	binary.Write(bfr, BIGEND, vloc.vpos)
	return bfr.Bytes()
}

func PackKey(key interface{}, debug *DebugLogger) []byte {
	bfr := new(bytes.Buffer)
	kbyts := InjectKeyType(key, debug)
	ksz := AsLBUINT(len(kbyts))
	binary.Write(bfr, BIGEND, ksz)
	bfr.Write(kbyts)
	return bfr.Bytes()
}

// Return a byte slice with a user permission record packed ready for file
// writing.
func PackUserPermissionRecord(key interface{}, upr *UserPermissionRecord, debug *DebugLogger) []byte {
	bfr := new(bytes.Buffer)
	kbyts := InjectKeyType(key, debug)
	ksz := AsLBUINT(len(kbyts))
	binary.Write(bfr, BIGEND, ksz)
	bfr.Write(kbyts)
	bfr.Write(PackPermission(upr))
	return bfr.Bytes()
}

func PackPermission(upr *UserPermissionRecord) []byte {
	return []byte{
		BoolToUint8(upr.Create) * PERMISSION_CREATE +
		BoolToUint8(upr.Read) * PERMISSION_READ +
		BoolToUint8(upr.Update) * PERMISSION_UPDATE +
		BoolToUint8(upr.Delete) * PERMISSION_DELETE,
	}
}

func BoolToUint8(x bool) uint8 {
	if x {return 1} else {return 0}
}

// Size calculations.

// Use reflection to find the byte size of any parameter, as an LBUINT.
func ParamSize(param interface{}) LBUINT {
	return LBUINT(reflect.TypeOf(param).Size())
}

// ValueLocations do not explicitely hold the start position and length
// of an entire logfile record, just the value, but along with the key we have
// enough to figure this out.
func (vloc *ValueLocation) ToRecordLocation(ksz LBUINT) *RecordLocation {
	rloc := NewRecordLocation()
	rloc.fnum = vloc.fnum
	rloc.rsz = LBUINT_SIZE_x2 + ksz + vloc.vsz + CRC_SIZE
	rloc.rpos = vloc.vpos - ksz - LBUINT_SIZE_x2
	return rloc
}

// Zapping.

// LBUINT division.
func Divide(dividend, divisor LBUINT) (q, rem LBUINT) {
	q = dividend / divisor
	rem = dividend % divisor
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
	debug.Basic("Purge zapmap of logfile %d entries", fnum)
	for key, zrecs := range zmap.zapmap {
		var newzrecs []*ZapRecord // Make a new list to replace old
		for _, zrec := range zrecs {
			if zrec.fnum != fnum {
				newzrecs = append(newzrecs, zrec)
			} else {
				debug.Fine("Deleting %q%s from zapmap", key, zrec.String())
			}
		}
		if len(newzrecs) == 0 {
			zmap.Delete(key)
		} else {
			zmap.Put(key, newzrecs)
		}
	}
	return
}

func Gobify(param interface{}, debug *DebugLogger) []byte {
	var bfr bytes.Buffer
	enc := gob.NewEncoder(&bfr)
	err := enc.Encode(param)
	debug.Error(err)
	return bfr.Bytes()
}

func Degobify(byts []byte, param interface{}, debug *DebugLogger) {
	var bfr bytes.Buffer
	dec := gob.NewDecoder(&bfr)
	err := dec.Decode(&param)
	debug.Error(err)
	return
}
