/*
	Defines and manages a catalog, of which the  Catalog is the most
   	important instance.  But the user can create a subset catalog of the 
	to speed up searches.  In other databases these are often called "Indexes".
*/
package logbase

import (
	"fmt"
	"bytes"
	"encoding/binary"
	"sync"
)

var nextCATID CATID_TYPE = CATID_MIN // A global  catalog record id counter
var queryCounter int = 0

const (
	CATALOG_FILENAME_PREFIX string = ".catalog_"
	CATID_MIN CATID_TYPE = 10 // Allow space for any special records
	QUERY_NAME_FORMAT string = "query_%06d"
)

// Define a record used in a logbase k-v map.
// Current implementors:
//  Value
//  ValueLocation
type CatalogRecord interface {
	Equals(other CatalogRecord) bool
	String() string
	ToValueLocation() *ValueLocation
	ReadVal(lbase *Logbase) (val []byte, vtype LBTYPE, err error)
}

// Catalog of all live (not stale) key-value pairs.
type Catalog struct {
	name		string
	ismaster	bool // Is this the Master Catalog?
	index		map[interface{}]CatalogRecord // The in-memory index
	file		*CatalogFile
	sync.RWMutex
	changed		bool // Has index changed since last save?
	nextid		CATID_TYPE
	update		bool // Update as logbase is changed?
	autosave	bool // Automatically save to file?
	debug		*DebugLogger
}

// Getters.

func (cat *Catalog) Name() string {return cat.name}
func (cat *Catalog) IsMaster() bool {return cat.ismaster}
func (cat *Catalog) Map() map[interface{}]CatalogRecord {return cat.index}
func (cat *Catalog) File() *CatalogFile {return cat.file}
func (cat *Catalog) HasChanged() bool {return cat.changed}
func (cat *Catalog) NextId() CATID_TYPE {return cat.nextid}
func (cat *Catalog) KeepUpdated() bool {return cat.update}
func (cat *Catalog) AutoSave() bool {return cat.autosave}

// Init a Catalog.
func MakeCatalog(name string, debug *DebugLogger) *Catalog {
	return &Catalog{
		name:		name,
		ismaster:	false,
		index:		make(map[interface{}]CatalogRecord),
		update:		false,
		autosave:	false,
		debug:		debug,
	}
}

func MakeMasterCatalog(debug *DebugLogger) *Catalog {
	return &Catalog{
		name:		MASTER_CATALOG_NAME,
		ismaster:	true,
		index:		make(map[interface{}]CatalogRecord),
		update:		true,
		autosave:	true,
		debug:		debug,
	}
}

func MakeQueryCatalog(debug *DebugLogger) *Catalog {
	return MakeCatalog(GetNextQueryCatalogName(), debug)
}

func GetNextQueryCatalogName() string {
	result := fmt.Sprintf(QUERY_NAME_FORMAT, queryCounter)
	queryCounter++
	return result
}

func (cat *Catalog) String() string {return cat.Name()}

// Return an existing cached Catalog, or assume it must be read from file.
func (lbase *Logbase) GetCatalog(name string) (*Catalog, error) {
	obj, present := lbase.CatalogCache().Get(name)
	if present {return obj.(*Catalog), nil}
	// Create from file
	cat := MakeCatalog(name, lbase.debug)
	err := cat.InitFile(lbase)
	lbase.debug.Error(err)
	cat.update = true
	cat.autosave = true
	lbase.CatalogCache().Put(name, cat)
	return cat, lbase.debug.Error(cat.Load(lbase))
}

// Initialise a Catalog file.  Not all catalogs need file service.
func (cat *Catalog) InitFile(lbase *Logbase) error {
	file, err := lbase.GetFile(cat.Filename())
	cat.debug.Error(err)
	file.Touch()
	cat.file = NewCatalogFile(file)
    return err
}

// Return the catalog filename.
func (cat *Catalog) Filename() string {
	return CATALOG_FILENAME_PREFIX + cat.Name()
}

// Update the Catalog, sort of intended for the Master Catalog only.
func (cat *Catalog) Update(key interface{}, cr CatalogRecord) CatalogRecord {
	cat.Put(key, cr)
	cat.SetNextId(key)
	return cr
}

// Gateway for reading from catalog.
func (cat *Catalog) Get(key interface{}) CatalogRecord {
	cat.RLock() // other reads ok
	cr := cat.index[key]
	cat.RUnlock()
	return cr
}

// Gateway for writing to catalog.
func (cat *Catalog) Put(key interface{}, cr CatalogRecord) {
	cat.Lock()
	cat.index[key] = cr
	cat.Unlock()
	cat.changed = true
	return
}

// Gateway for removing entry from catalog.
func (cat *Catalog) Delete(key interface{}) {
	cat.Lock()
	delete(cat.index, key)
	cat.Unlock()
	cat.changed = true
	return
}

// Wrapper for Id.

type CatalogId struct {
	id		CATID_TYPE
}

func NewCatalogId(id CATID_TYPE) *CatalogId {
	return &CatalogId{id}
}

// Compare for equality against another CatalogId.
func (cid *CatalogId) Equals(other *CatalogId) bool {
	if other == nil {return false}
	return (cid.id == other.id)
}

// Return string representation of a CatalogId.
func (cid *CatalogId) String() string {
	return fmt.Sprintf("%d", cid.id)
}

// Read the value pointed to by the CatalogId.
func (cid *CatalogId) ReadVal(lbase *Logbase) ([]byte, LBTYPE, error) {
	mcr := lbase.MasterCatalog().index[cid.id]
	vloc, ok := mcr.(*ValueLocation)
	if ok {return vloc.ReadVal(lbase)}
	err := FmtErrBadType(
			"The  Catalog id %v points to another id %v, " +
			"which is prohibited",
			cid.id, vloc)
	return nil, LBTYPE_NIL, err
}

// Return a byte slice with a CatalogId packed ready for file writing.
func (cid *CatalogId) Pack(key interface{}, debug *DebugLogger) []byte {
	bfr := new(bytes.Buffer)
	byts := PackKey(key, debug)
	bfr.Write(byts)
	binary.Write(bfr, BIGEND, LBTYPE_CATID)
	binary.Write(bfr, BIGEND, cid.id)
	return bfr.Bytes()
}

// Return the byte slice representation of a CatalogId.
func (cid *CatalogId) ToBytes(debug *DebugLogger) []byte {
	bfr := new(bytes.Buffer)
	binary.Write(bfr, BIGEND, cid.id)
	return bfr.Bytes()
}

//  Set of catalog Ids.

type CatalogIdSet struct {
	set		[]*CatalogId
}

func NewCatalogIdSet() *CatalogIdSet {
	return &CatalogIdSet{}
}

func MakeCatalogIdSet(id CATID_TYPE) *CatalogIdSet {
	return &CatalogIdSet{
		[]*CatalogId{NewCatalogId(id)},
	}
}

// Compare for equality against another CatalogIdSet.
func (cidset *CatalogIdSet) Equals(other *CatalogIdSet) bool {
	if other == nil {return false}
	if len(cidset.set) != len(other.set) {return false}
	result := false
	for i, cid := range cidset.set {
		result = result && (cid.Equals(other.set[i]))
	}
	return result
}

// Return string representation of a CatalogIdSet.
func (cidset *CatalogIdSet) String() string {
	result := "["
	for i, cid := range cidset.set {
		if i > 0 {result += ","}
		result += fmt.Sprintf("%s", cid.String())
	}
	return result + "]"
}

// Return a byte slice with a CatalogIdSet packed ready for file writing.
func (cidset *CatalogIdSet) Pack(debug *DebugLogger) []byte {
	bfr := new(bytes.Buffer)
	binary.Write(bfr, BIGEND, LBTYPE_CATID_SET)
	for _, cid := range cidset.set {
		binary.Write(bfr, BIGEND, cid.id)
	}
	return bfr.Bytes()
}

func (cidset *CatalogIdSet) ToBytes(debug *DebugLogger) []byte {
	bfr := new(bytes.Buffer)
	for _, cid := range cidset.set {
		binary.Write(bfr, BIGEND, cid.id)
	}
	return bfr.Bytes()
}

// Read the parent CATID set, which is always the last section of the buffer.
func (cidset *CatalogIdSet) FromBytes(bfr *bytes.Buffer, debug *DebugLogger) (err error) {
	rem := bfr.Len() % int(CATID_TYPE_SIZE)
	if rem > 0 {
		err = debug.Error(FmtErrPartialCATIDSet(bfr.Len(), CATID_TYPE_SIZE))
		return
	}
	n := bfr.Len() / int(CATID_TYPE_SIZE)
	var id CATID_TYPE
	cidset.set = make([]*CatalogId, n)
	for i := 0; i < n; i++ {
		err = debug.Error(binary.Read(bfr, BIGEND, &id))
		cidset.set[i] = NewCatalogId(id)
		if err != nil {break}
	}
	return
}

// Does the set contain the given CATID?
func (cidset *CatalogIdSet) Contains(othercid *CatalogId) bool {
	for _, cid := range cidset.set {
		if othercid.Equals(cid) {return true}
	}
	return false
}

// Append given CATID to set if it is not already present.
func (cidset *CatalogIdSet) Add(othercid *CatalogId) {
	for _, cid := range cidset.set {
		if othercid.Equals(cid) {return} // Already exists
	}
	cidset.set = append(cidset.set, othercid)
	return
}

// Comparison.

// Compare for equality against CatalogRecord interface.
func (vloc *ValueLocation) Equals(other CatalogRecord) bool {
	if other == nil {return false}
	if othervloc, ok := other.(*ValueLocation); ok {
		return (vloc.fnum == othervloc.fnum &&
			vloc.vsz == othervloc.vsz &&
			vloc.vpos == othervloc.vpos)
	}
	return false
}

// Compare for equality against CatalogRecord interface.
func (val *Value) Equals(other CatalogRecord) bool {
	if other == nil {return false}
	return val.ValueLocation.Equals(other.ToValueLocation())
}

// Read an CATID from a byte slice.
func BytesToCatalogId(byts []byte, debug *DebugLogger) (interface{}, error) {
	bfr := bytes.NewBuffer(byts)
	var cid CATID_TYPE
	err := debug.DecodeError(binary.Read(bfr, BIGEND, &cid))
	return cid, err
}

// Catalog id counter.

// Reset the next CATID to the minimum value.
func (cat *Catalog) ResetId() {
	cat.nextid = CATID_MIN
	return
}

// Increment the CATID counter by one.
func (cat *Catalog) IncNextId() CATID_TYPE {
	cat.nextid++
	return cat.nextid
}

// Get and increment next CATID counter value.
func (cat *Catalog) PopNextId() CATID_TYPE {
	n := cat.NextId()
	cat.IncNextId()
	return n
}

// Test whether the given key value is of type CATID_TYPE.
func IsCATID(key interface{}) bool {
	_, isCATID := key.(CATID_TYPE)
	if isCATID {return true}
	return false
}

// If the given key value is of the correct type, increment the CATID counter.
func (cat *Catalog) SetNextId(key interface{}) {
	if cid, isCATID := key.(CATID_TYPE); isCATID {
		cat.nextid = cid + 1
	}
	return
}
