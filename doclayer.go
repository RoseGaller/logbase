/*
	Defines and manages documents.

	OPTION 1
	API example (LB+ means added as k-v pair to logbase)

	colour := NewKind("Colour", nil)
    green := NewDoc("Colour", "green")
	green.Save()
	// LB+ "/Colour/green" -> LBTYPE_NIL()

	thing := NewKind("Thing", nil)
	animal := NewKind("Animal", thing)

	frog := NewDoc("Animal", "frog")
	frog.SetField("eyes", uint8(2))
	frog.SetField("colour", green)
	frog.Save()
	// LB+ "/Thing/Animal/" -> LBTYPE_NIL()
	// LB+ "/Thing/Animal/frog.eyes" -> LBTYPE_UINT8(2)
  	// LB+ "/Thing/Animal/frog.colour" -> LBTYPE_MCK("/Colour/green")
	// LB+ "/Thing/Animal/frog" -> LBTYPE_STRING("eyes;colour")

	f1 := GetDoc(animal, "frog") // Finds "/Thing/Animal/frog" and retrieves each field into struct

	OPTION 2
	API example (LB+ means added as k-v pair to logbase)

	colour := NewKind("Colour")
	// Doesn't exist, MCID_MIN = 10
	// LB+ LBTYPE_MCIDS(10) -> LBTYPE_MCK("Colour")
	// LB+ "Colour" -> LBTYPE_MCIDSET(10)
	colour.Parent = GetKind(
    green := NewDoc("Colour", "green")
	green.Save()

	thing := NewKind("Thing", nil)
	animal := NewKind("Animal", thing)

	frog := NewDoc(animal, "frog")
	frog.SetField("eyes", uint8(2))
	frog.SetField("colour", green)
	frog.Save()
	// LB+ LBTYPE_MCIDS(2) -> LBTYPE_MCK("/Thing/Animal/")
	// LB+ "/Thing/Animal/" -> LBTYPE_MCIDS(2)
	// LB+ LBTYPE_MCIDS(3) -> VALOC -> LBTYPE_DOC(LBTYPE_MCK("Thing/Animal/frog"),LBTYPE_MAP("colour":LBTYPE_MCIDS(1),"eyes":LBTYPE_UINT8(2)))
	// LB+ "/Thing/Animal/frog" -> LBTYPE_MCIDS(3)

	f1 := GetDoc(animal, "frog") // Finds "/Thing/Animal/frog" and retrieves each field into struct
*/
package logbase

import (
	"fmt"
	"bufio"
	"encoding/binary"
	"bytes"
)

var nextMCID MCID_TYPE = MCID_MIN // A global Master catalog record id counter
const MCID_MIN MCID_TYPE = 10 // Allow space for any special records

// A Kind is the heart of a future schema infrastructure.
type Kind struct {
	name		string
	parents		*MasterCatalogIdSet
}

type Field struct {
	lbtype	LBTYPE
	val		interface{}
}

type FieldMap struct {
	fields	map[interface{}]*Field
}

type Document struct {
	name	string
	kind	*Kind
	*FieldMap
}

type MasterCatalogId struct {
	id		MCID_TYPE
}

func NewMasterCatalogId() *MasterCatalogId {
	return &MasterCatalogId{}
}

type MasterCatalogIdSet struct {
	set		[]MCID_TYPE // Ordered list, with the 0th item the identity
}

func NewMasterCatalogIdSet() *MasterCatalogIdSet {
	return &MasterCatalogIdSet{}
}

// Compare for equality against another MasterCatalogId.
func (mcid *MasterCatalogId) Equals(other *MasterCatalogId) bool {
	if other == nil {return false}
	return (mcid.id == other.id)
}

// Compare for equality against another MasterCatalogIdSet.
func (mcidset *MasterCatalogIdSet) Equals(other *MasterCatalogIdSet) bool {
	if other == nil {return false}
	if len(mcidset.set) != len(other.set) {return false}
	result := false
	for i, id := range mcidset.set {
		result = result && (id == other.set[i])
	}
	return result
}

// Return string representation of a MasterCatalogId.
func (mcid *MasterCatalogId) String() string {
	return fmt.Sprintf("%d", mcid.id)
}

// Return string representation of a MasterCatalogIdSet.
func (mcidset *MasterCatalogIdSet) String() string {
	result := "("
	for i, id := range mcidset.set {
		if i > 0 {result += ","}
		result += fmt.Sprintf("%d", id)
	}
	return result + ")"
}

// Read the value pointed to by the MasterCatalogId.
func (mcid *MasterCatalogId) ReadVal(lbase *Logbase) ([]byte, LBTYPE, error) {
	mcr := lbase.mcat.index[mcid.id]
	vloc, ok := mcr.(*ValueLocation)
	if ok {return vloc.ReadVal(lbase)}
	err := FmtErrBadType(
			"The Master Catalog id %v points to another id %v, " +
			"which is prohibited",
			mcid.id, vloc)
	return nil, LBTYPE_NIL, err
}

// Return a byte slice with a MasterCatalogId packed ready for file writing.
func (mcid *MasterCatalogId) Pack(key interface{}, debug *DebugLogger) []byte {
	bfr := new(bytes.Buffer)
	byts := PackKey(key, debug)
	bfr.Write(byts)
	binary.Write(bfr, BIGEND, LBTYPE_MCID)
	binary.Write(bfr, BIGEND, mcid.id)
	return bfr.Bytes()
}

// Return a byte slice with a MasterCatalogIdSet packed ready for file writing.
func (mcidset *MasterCatalogIdSet) Pack(debug *DebugLogger) []byte {
	bfr := new(bytes.Buffer)
	binary.Write(bfr, BIGEND, LBTYPE_MCID_SET)
	for _, id := range mcidset.set {
		binary.Write(bfr, BIGEND, id)
	}
	return bfr.Bytes()
}

// Kind.


func (lbase *Logbase) NewKind(name string) (kind *Kind, err error) {
	vbyts, vtype, err := lbase.Get(name)
	if err != nil {return}
	if vbyts == nil {
		v := InjectType([]byte(name), LBTYPE_MCK)
		id := GetNextMCID()
		lbase.Put(id, v, LBTYPE_MCK)
		kind = &Kind{name, &MasterCatalogIdSet{[]MCID_TYPE{id}}}
		kind.Put(lbase)
	} else {
		if vtype != LBTYPE_MCID_SET {
			err = lbase.debug.Error(FmtErrBadType(
				"Found record in logbase %s for kind %q with type %v, but should be %v",
				lbase.name, name, vtype, LBTYPE_MCID_SET))
			return
		}
		rem := len(vbyts) % int(MCID_TYPE_SIZE)
		if rem > 0 {
			err = lbase.debug.Error(ErrDataSize(
				"The MCID set for kind %q in logbase %s has length %d which is not a " +
				"multiple of the MCID type size of %d",
				name, lbase.name, len(vbyts), MCID_TYPE_SIZE))
			return
		}
		n := len(vbyts) / int(MCID_TYPE_SIZE)
		kind = &Kind{name, &MasterCatalogIdSet{make([]MCID_TYPE, n)}}
		bfr := bufio.NewReader(bytes.NewBuffer(vbyts))
		for i := 0; i < n; i++ {
			binary.Read(bfr, BIGEND, &kind.parents.set[i])
		}
	}
	return
}

func (kind *Kind) Put(lbase *Logbase) error {
	_, err := lbase.Put(kind.name, kind.parents.Pack(lbase.debug), LBTYPE_MCID_SET)
	return err
}

//func (kind *Kind) AddParent(parent *Kind) {
//
//}

func NewFieldMap() *FieldMap {
	return &FieldMap{
		fields: make(map[interface{}]*Field),
	}
}

func NewDoc(kind *Kind, name string) *Document {
	return &Document{
		name: name,
		kind: kind,
		FieldMap: NewFieldMap(),
	}
}

// Pack/unpack document to/from []byte value. 

//func (doc *Document) Pack() []byte {
//	bfr := new(bytes.Buffer)
//}
//
//// Pack a FieldMap into bytes ready for disk.
//func (fmap *FieldMap) Pack(debug *DebugLogger) []byte {
//	bfr := new(bytes.Buffer)
//	for k, f := range fmap.fields {
//		kbyts := InjectKeyType(k, debug)
//		vbyts := InjectType(f.val, f.lbtype)
//		binary.Write(bfr, BIGEND, AsLBUINT(len(kbyts)))
//		bfr.Write(kbyts)
//		binary.Write(bfr, BIGEND, AsLBUINT(len(vbyts)))
//		bfr.Write(vbyts)
//	}
//}

// Master Catalog id counter.

// Reset the next MCID to the minimum value.
func ResetMCID() {
	nextMCID = MCID_MIN
	return
}

// Increment the MCID counter by one.
func IncMCID() MCID_TYPE {
	nextMCID = nextMCID + 1
	return nextMCID
}

// Getter for next MCID counter value.
func GetNextMCID() MCID_TYPE {return nextMCID}

// Test whether the given key value is of type MCID_TYPE.
func IsMCID(key interface{}) bool {
	_, isMCID := key.(MCID_TYPE)
	if isMCID {return true}
	return false
}

// If the given key value is of the correct type, increment the MCID counter.
func IncIfMCID(key interface{}) bool {
	if IsMCID(key) {
		IncMCID()
		return true
	}
	return false
}

func (lbase *Logbase) InitDocCat() {
	return
}
