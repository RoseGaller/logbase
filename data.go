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

// Whether to read a value size after the key size when using the
// GenericRecord to read these various record types.
var DoReadDataValueSize = map[int]bool{
	LOG_RECORD:			true,
	INDEX_RECORD:		false,
	MASTER_RECORD:		false,
	ZAP_RECORD:			true,
	PERMISSION_RECORD:  false,
}

// Whether to snip the type of a value from the stored value bytes.
var DoSnipValueType = map[int]bool{
	LOG_RECORD:			true,
	INDEX_RECORD:		false,
	MASTER_RECORD:		false,
	ZAP_RECORD:			false,
	PERMISSION_RECORD:	false,
}

// Data containers.

// Key data container.
type Kdata struct {
	key     []byte
}

// Key type.
type Ktype struct {
	ktype   LBTYPE
}

// Value data container.
type Vdata struct {
	val     []byte
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
	*Ksize  // typed key, that is including LBTYPE
	*Vsize  // typed value, that is including LBTYPE
	*Kdata
	*Ktype
	*Vdata
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
	*Ksize  // typed key, that is including LBTYPE
	*Vsize	// typed value, that is including LBTYPE
	*Kdata
	*Ktype
	*Vdata
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

// Used by ReadRecord to read generic (packed) value size.
var GenericValueSize = map[int]LBUINT{
	LOG_RECORD:			0, // not used
	INDEX_RECORD:		ValSizePosBytes(),
	MASTER_RECORD:		ValueLocationBytes(),
	ZAP_RECORD:			0, // not used
	PERMISSION_RECORD:  PermissionBytes(),
}

func ValSizePosBytes() LBUINT {return LBUINT_SIZE_x2}
func ValueLocationBytes() LBUINT {return LBUINT_SIZE_x3}
func RecordLocationBytes() LBUINT {return LBUINT_SIZE_x3}
func PermissionBytes() LBUINT {return LBUINT(1)}

// Logbase level.

//  Index of all key-value pairs in a log file.
type Index struct {
	list    []*IndexRecord
}

//  Master catalog of all live (not stale) key-value pairs.
type MasterCatalog struct {
	index   map[interface{}]*MasterCatalogRecord // The in-memory index
	file    *Masterfile
	sync.RWMutex
	changed	bool // Has index changed since last save?
}

// Init a MasterCatalog.
func NewMasterCatalog() *MasterCatalog {
	return &MasterCatalog{
		index: make(map[interface{}]*MasterCatalogRecord),
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

// Update the master catalog map with an index record (usually) generated from
// an individual log file, and add an existing (stale) value entry to the
// zapmap.
func (lbase *Logbase) Update(irec *IndexRecord, fnum LBUINT) *MasterCatalogRecord {
	newmcr := NewMasterCatalogRecord()
	newmcr.FromIndexRecord(irec, fnum)
	key := GetKey(irec.key, irec.ktype, lbase.debug)
	oldmcr := lbase.mcat.Get(key)

	if oldmcr != nil {
		// Add to zapmap
		zrec := NewZapRecord()
		zrec.FromMasterCatalogRecord(irec.ksz, oldmcr)
		lbase.zmap.PutRecord(key, zrec)
	}

	// Update the master catalog
	lbase.mcat.Put(key, newmcr)
	return newmcr
}

// Gateway for reading from master catalog.
func (mcat *MasterCatalog) Get(key interface{}) *MasterCatalogRecord {
	mcat.RLock() // other reads ok
	mcr := mcat.index[key]
	mcat.RUnlock()
	return mcr
}

// Gateway for writing to master catalog.
func (mcat *MasterCatalog) Put(key interface{}, mcr *MasterCatalogRecord) {
	mcat.Lock()
	mcat.index[key] = mcr
	mcat.Unlock()
	mcat.changed = true
	return
}

// Gateway for removing entire entry from master catalog.
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

// Unpacking and interchanges.

// Inject type byte(s) at start of key or value byte slice.
func InjectType(x []byte, typ LBTYPE) []byte {
	bfr := new(bytes.Buffer)
	binary.Write(bfr, BIGEND, typ)
	bfr.Write(x)
	return bfr.Bytes()
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

func SnipValueType(val []byte) (newval []byte, vtype LBTYPE) {
	bfr := bufio.NewReader(bytes.NewBuffer(val[:LBTYPE_SIZE]))
	binary.Read(bfr, BIGEND, &vtype)
	newval = val[LBTYPE_SIZE:]
	return
}

func SnipKeyType(key []byte)  (newkey []byte, ktype LBTYPE) {
	bfr := bufio.NewReader(bytes.NewBuffer(key[:LBTYPE_SIZE]))
	binary.Read(bfr, BIGEND, &ktype)
	newkey	= key[LBTYPE_SIZE:]
	return
}

func (vl *ValueLocation) FromIndexRecord(irec *IndexRecord, fnum LBUINT) {
	vl.fnum = fnum
	vl.vsz = irec.vsz
	vl.vpos = irec.vpos
}

func (zrec *ZapRecord) FromMasterCatalogRecord(ksz LBUINT, mcr *MasterCatalogRecord) {
	zrec.fnum = mcr.fnum
	rsz, rpos := mcr.LogRecordSizePosition(ksz)
	zrec.rsz = rsz
	zrec.rpos = rpos
}

// Map GenericRecord to a new LogRecord.
func (rec *GenericRecord) ToLogRecord() *LogRecord {
	lrec := NewLogRecord()
	lrec.ksz = rec.ksz
	lrec.vsz = rec.vsz - CRC_SIZE
	lrec.key = rec.key
	lrec.ktype = rec.ktype
	lrec.vtype = rec.vtype
	// Unpack
	bfr := bufio.NewReader(bytes.NewBuffer(rec.val))
	lrec.val = make([]byte, lrec.vsz) // must have fixed size
	binary.Read(bfr, BIGEND, &lrec.val)
	binary.Read(bfr, BIGEND, &lrec.crc)
	return lrec
}

// Map GenericRecord to a new IndexRecord.
func (rec *GenericRecord) ToIndexRecord() *IndexRecord {
	irec := NewIndexRecord()
	irec.ksz = rec.ksz
	irec.key = rec.key
	irec.ktype = rec.ktype
	// Unpack
	bfr := bufio.NewReader(bytes.NewBuffer(rec.val))
	binary.Read(bfr, BIGEND, &irec.vsz)
	binary.Read(bfr, BIGEND, &irec.vpos)
	return irec
}

// Map LogRecord to an IndexRecord.  Note vpos is left as nil.
func (lrec *LogRecord) ToIndexRecord() *IndexRecord {
	irec := NewIndexRecord()
	irec.ksz = lrec.ksz
	irec.vsz = lrec.vsz
	irec.key = lrec.key
	irec.ktype = lrec.ktype
	return irec
}

// Map GenericRecord to a new MasterLogRecord.
func (rec *GenericRecord) ToMasterCatalogRecord(debug *DebugLogger) (interface{}, *MasterCatalogRecord) {
	key := GetKey(rec.key, rec.ktype, debug)
	mcr := NewMasterCatalogRecord()
	// Unpack
	bfr := bufio.NewReader(bytes.NewBuffer(rec.val))
	binary.Read(bfr, BIGEND, &mcr.fnum)
	binary.Read(bfr, BIGEND, &mcr.vsz)
	binary.Read(bfr, BIGEND, &mcr.vpos)
	return key, mcr
}

// Map GenericRecord to a new ZapRecord list.
func (rec *GenericRecord) ToZapRecordList(debug *DebugLogger) (interface{}, []*ZapRecord) {
	key := GetKey(rec.key, rec.ktype, debug)
	n := rec.LocationListLength()
	bfr := bufio.NewReader(bytes.NewBuffer(rec.val))
	var zrecs = make([]*ZapRecord, n)
	for i := 0; i < n; i++ {
		zrecs[i] = NewZapRecord()
	    binary.Read(bfr, BIGEND, &zrecs[i].fnum)
	    binary.Read(bfr, BIGEND, &zrecs[i].rsz)
	    binary.Read(bfr, BIGEND, &zrecs[i].rpos)
	}
	return key, zrecs
}

// Map GenericRecord to a new UserPermissionRecord.
func (rec *GenericRecord) ToUserPermissionRecord(debug *DebugLogger) (interface{}, *UserPermissionRecord) {
	key := GetKey(rec.key, rec.ktype, debug)
	upr := NewUserPermissionRecord()
	// Unpack
	var p uint8 = uint8(rec.val[0])
	upr.Create = PERMISSION_CREATE == (p & PERMISSION_CREATE)
	upr.Read = PERMISSION_READ == (p & PERMISSION_READ)
	upr.Update = PERMISSION_UPDATE == (p & PERMISSION_UPDATE)
	upr.Delete = PERMISSION_DELETE == (p & PERMISSION_DELETE)
	fmt.Printf("CHECK key=%v upr=%v\n", key, upr)
	return key, upr
}

// Formatted output for debugging.

// Return string representation of a GenericRecord for debugging.
func (rec *GenericRecord) String() string {
	return fmt.Sprintf(
		"(ksz=%d vsz=%d key=%q ktype=%d val=%q vtype=%d vpos=%d)",
		rec.ksz,
		rec.vsz,
		string(rec.key),
		rec.ktype,
		string(rec.val),
		rec.vtype,
		rec.vpos)
}

// Return string representation of a GenericRecord for debugging.
func (lrec *LogRecord) String() string {
	return fmt.Sprintf(
		"(ksz=%d vsz=%d key=%q ktype=%d val=%q vtype=%d crc=%d)",
		lrec.ksz,
		lrec.vsz,
		string(lrec.key),
		lrec.ktype,
		string(lrec.val),
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
		string(irec.key),
		irec.ktype)
}

// Return string representation of a MasterCatalogRecord.
func (mcr *MasterCatalogRecord) String() string {
	return fmt.Sprintf(
		"(fnum=%d vpos=%d vsz=%d)",
		mcr.fnum,
		mcr.vpos,
		mcr.vsz)
}

// Return string representation of a ZapRecord.
func (zrec *ZapRecord) String() string {
	return fmt.Sprintf(
		"(fnum=%d rpos=%d rsz=%d)",
		zrec.fnum,
		zrec.rpos,
		zrec.rsz)
}

// LBUINT related functions.

func (i LBUINT) Plus(other int) LBUINT {
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

func MakeLogRecord(key interface{}, val []byte, vtype LBTYPE, debug *DebugLogger) *LogRecord {
	lrec := NewLogRecord()
	ktype := GetKeyType(key, debug)
	kbyts := KeyToBytes(key)
	lrec.key = kbyts
	lrec.ktype = ktype
	lrec.ksz = AsLBUINT(len(kbyts) + LBTYPE_SIZE)
	lrec.val = val
	lrec.vtype = vtype
	lrec.vsz = AsLBUINT(len(val) + LBTYPE_SIZE)
	return lrec
}

// Return a byte slice with a log record packed ready for file writing.
func (lrec *LogRecord) Pack() []byte {
	bfr := new(bytes.Buffer)
	binary.Write(bfr, BIGEND, lrec.ksz)
	binary.Write(bfr, BIGEND, lrec.vsz + CRC_SIZE)
	bfr.Write(InjectType(lrec.key, lrec.ktype))
	bfr.Write(InjectType(lrec.val, lrec.vtype))

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
	bfr.Write(InjectType(irec.key, irec.ktype))
	binary.Write(bfr, BIGEND, irec.vsz)
	binary.Write(bfr, BIGEND, irec.vpos)
	return bfr.Bytes()
}

// Return a byte slice with a zapmap record packed ready for file writing.
func PackZapRecord(key interface{}, zrecs []*ZapRecord, debug *DebugLogger) []byte {
	bfr := new(bytes.Buffer)
	kbyts := InjectKeyType(key, debug)
	ksz := AsLBUINT(len(kbyts))
	n := AsLBUINT(len(zrecs)) * ValueLocationBytes()
	binary.Write(bfr, BIGEND, ksz)
	binary.Write(bfr, BIGEND, n)
	bfr.Write(kbyts)
	for _, zrec := range zrecs {
	    binary.Write(bfr, BIGEND, zrec.fnum)
	    binary.Write(bfr, BIGEND, zrec.rsz)
	    binary.Write(bfr, BIGEND, zrec.rpos)
	}
	return bfr.Bytes()
}

// Return a byte slice with a master catalog record packed ready for file
// writing.
func PackMasterRecord(key interface{}, mcr *MasterCatalogRecord, debug *DebugLogger) []byte {
	bfr := new(bytes.Buffer)
	kbyts := InjectKeyType(key, debug)
	ksz := AsLBUINT(len(kbyts))
	binary.Write(bfr, BIGEND, ksz)
	bfr.Write(kbyts)
	binary.Write(bfr, BIGEND, mcr.fnum)
	binary.Write(bfr, BIGEND, mcr.vsz)
	binary.Write(bfr, BIGEND, mcr.vpos)
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

// MasterCatalogRecords do not explicitely hold the start position and length
// of an entire logfile record, just the value, but along with the key we have
// enough to figure this out.
func (mcr *MasterCatalogRecord) LogRecordSizePosition(ksz LBUINT) (size, pos LBUINT) {
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
