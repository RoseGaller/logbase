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

	colour := NewKind("Colour", nil)
    green := NewDoc("Colour", "green")
	green.Save()
	// LB+ LBTYPE_MCID(1) -> LBTYPE_MCK("/Colour/green")
	// LB+ "/Colour/green" -> LBTYPE_MCID(1)

	thing := NewKind("Thing", nil)
	animal := NewKind("Animal", thing)

	frog := NewDoc(animal, "frog")
	frog.SetField("eyes", uint8(2))
	frog.SetField("colour", green)
	frog.Save()
	// LB+ LBTYPE_MCID(2) -> LBTYPE_MCK("/Thing/Animal/")
	// LB+ "/Thing/Animal/" -> LBTYPE_MCID(2)
	// LB+ LBTYPE_MCID(3) -> LBTYPE_DOC(LBTYPE_MCK("Thing/Animal/frog"),LBTYPE_MAP("colour":LBTYPE_MCID(1),"eyes":LBTYPE_UINT8(2)))
	// LB+ "/Thing/Animal/frog" -> LBTYPE_MCID(3)

	f1 := GetDoc(animal, "frog") // Finds "/Thing/Animal/frog" and retrieves each field into struct
*/
package logbase

import (
	//"sync"
	//"strings"
)

var NextMCID MCID_TYPE = MinMCID // A global Master catalog record id counter
const MinMCID MCID_TYPE = 10 // Allow space for any special records

// A Kind is identified by a path much like a directory in a file system.  In
// same way, multiple paths can lead to the same Kind, allowing multiple
// inheritance of schema.
type Kind struct {
	mcid	[]MCID_TYPE
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

////
//func (lbase *Logbase) FindKind(name string) []MCID_TYPE {
//	kind := name + "/"
//	for k := range lbase.mcat.index {
//		switch k.(type) {
//		case string:
//			if strings.HasSuffix(k, kind) {
//				v := lbase.mcat.index[k]
//				switch v.(type) {
//				case MCID_TYPE 
//				}
//			}
//		}
//	}
//	return &Kind{name, parent}
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

//func GetKindPath(name string, parent *Kind) string {
//	var path string
//	if parent == nil {
//		path = "/" + name
//	} else {
//
//	}
//
//}

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
	NextMCID = MinMCID
	return
}

// Increment the MCID counter by one.
func IncMCID() MCID_TYPE {
	NextMCID = NextMCID + 1
	return NextMCID
}

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
