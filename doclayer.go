/*
	Defines and manages Nodes, which can include Kinds or Documents.

	colour := Kind("Colour")
	// Doesn't exist, MCID_MIN = 10
	// LB+ LBTYPE_MCID(10) -> LBTYPE_KIND(LBTYPE_MCK("Colour"),LBTYPE_MCID_SET(10))
	// LB+ "Colour" -> LBTYPE_MCID(10)
    green := Kind("Green")

	green.Save()

	thing, _, err := NewKind("Thing")
	animal, _, err := NewKind("Animal")
	animal.AddParent(thing).Save()
	thing.Save()
	animal.Save()

	frog := NewDoc("frog", animal)
	frog.SetField("eyes", uint8(2))
	frog.SetField("colour", green)
	frog.Save()
	// LB+ LBTYPE_MCID(11) -> LBTYPE_MCK("/Thing/Animal/")
	// LB+ "/Thing/Animal/" -> LBTYPE_MCID(11)
	// LB+ LBTYPE_MCIDS(12) -> VALOC -> LBTYPE_DOC(LBTYPE_MCK("Thing/Animal/frog"),LBTYPE_MAP("colour":LBTYPE_MCIDS(1),"eyes":LBTYPE_UINT8(2)))
	// LB+ "/Thing/Animal/frog" -> LBTYPE_MCIDS(12)

	f1 := GetDoc(animal, "frog") // Finds "/Thing/Animal/frog" and retrieves each field into struct
*/
package logbase

import (
	"fmt"
	"encoding/binary"
	"bytes"
	"strings"
	"io"
)

var nextMCID MCID_TYPE = MCID_MIN // A global Master catalog record id counter
const MCID_MIN MCID_TYPE = 10 // Allow space for any special records

type BYTES []byte

type NodeConfig struct {
	namespace	string
}

var NodeConfigs = map[LBTYPE]*NodeConfig{
	LBTYPE_KIND:	&NodeConfig{"kind"},
	LBTYPE_DOC:		&NodeConfig{"doc"},
}

// A Node can represent a "kind" (type or class) or a "document".
type Node struct {
	mcid		*MasterCatalogId
	name		string // MC string key stored with namespace prefix
	parents		*MasterCatalogIdSet
	ntype		LBTYPE
	*FieldMap
	debug		*DebugLogger // a small price to pay
}

type Field struct {
	*Vtype
	*Vdata // does not include LBTYPE
}

type FieldMap struct {
	fields	map[string]*Field
}

type MasterCatalogId struct {
	id		MCID_TYPE
}

type MasterCatalogIdSet struct {
	set		[]*MasterCatalogId // Ordered list, with the 0th item the identity
}

// Master Catalog ID.

func NewMasterCatalogId(id MCID_TYPE) *MasterCatalogId {
	return &MasterCatalogId{id}
}

// Compare for equality against another MasterCatalogId.
func (mcid *MasterCatalogId) Equals(other *MasterCatalogId) bool {
	if other == nil {return false}
	return (mcid.id == other.id)
}

// Return string representation of a MasterCatalogId.
func (mcid *MasterCatalogId) String() string {
	return fmt.Sprintf("%d", mcid.id)
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

// Return the byte slice representation of a MasterCatalogId.
func (mcid *MasterCatalogId) ToBytes(debug *DebugLogger) []byte {
	bfr := new(bytes.Buffer)
	binary.Write(bfr, BIGEND, mcid.id)
	return bfr.Bytes()
}

// Read an MCID from a byte slice.
func (byts BYTES) ToIdKey(debug *DebugLogger) (interface{}, error) {
	bfr := bytes.NewBuffer(byts)
	var mcid MCID_TYPE
	err := debug.DecodeError(binary.Read(bfr, BIGEND, &mcid))
	return mcid, err
}

// Master Catalog ID Set.

func NewMasterCatalogIdSet() *MasterCatalogIdSet {
	return &MasterCatalogIdSet{}
}

func MakeMasterCatalogIdSet(id MCID_TYPE) *MasterCatalogIdSet {
	return &MasterCatalogIdSet{
		[]*MasterCatalogId{NewMasterCatalogId(id)},
	}
}

// Compare for equality against another MasterCatalogIdSet.
func (mcidset *MasterCatalogIdSet) Equals(other *MasterCatalogIdSet) bool {
	if other == nil {return false}
	if len(mcidset.set) != len(other.set) {return false}
	result := false
	for i, mcid := range mcidset.set {
		result = result && (mcid.Equals(other.set[i]))
	}
	return result
}

// Return string representation of a MasterCatalogIdSet.
func (mcidset *MasterCatalogIdSet) String() string {
	result := "["
	for i, mcid := range mcidset.set {
		if i > 0 {result += ","}
		result += fmt.Sprintf("%s", mcid.String())
	}
	return result + "]"
}

// Return a byte slice with a MasterCatalogIdSet packed ready for file writing.
func (mcidset *MasterCatalogIdSet) Pack(debug *DebugLogger) []byte {
	bfr := new(bytes.Buffer)
	binary.Write(bfr, BIGEND, LBTYPE_MCID_SET)
	for _, mcid := range mcidset.set {
		binary.Write(bfr, BIGEND, mcid.id)
	}
	return bfr.Bytes()
}

func (mcidset *MasterCatalogIdSet) ToBytes(debug *DebugLogger) []byte {
	bfr := new(bytes.Buffer)
	for _, mcid := range mcidset.set {
		binary.Write(bfr, BIGEND, mcid.id)
	}
	return bfr.Bytes()
}

// Read the parent MCID set, which is always the last section of the buffer.
func (mcidset *MasterCatalogIdSet) FromBytes(bfr *bytes.Buffer, debug *DebugLogger) (err error) {
	rem := bfr.Len() % int(MCID_TYPE_SIZE)
	if rem > 0 {
		err = debug.Error(FmtErrPartialMCIDSet(bfr.Len(), MCID_TYPE_SIZE))
		return
	}
	n := bfr.Len() / int(MCID_TYPE_SIZE)
	var id MCID_TYPE
	mcidset.set = make([]*MasterCatalogId, n)
	for i := 0; i < n; i++ {
		err = debug.Error(binary.Read(bfr, BIGEND, &id))
		mcidset.set[i] = NewMasterCatalogId(id)
		if err != nil {break}
	}
	return
}

// Does the set contain the given MCID?
func (mcidset *MasterCatalogIdSet) Contains(othermcid *MasterCatalogId) bool {
	for _, mcid := range mcidset.set {
		if othermcid.Equals(mcid) {return true}
	}
	return false
}

// Append given MCID to set if it is not already present.
func (mcidset *MasterCatalogIdSet) Add(othermcid *MasterCatalogId) {
	for _, mcid := range mcidset.set {
		if othermcid.Equals(mcid) {return} // Already exists
	}
	mcidset.set = append(mcidset.set, othermcid)
	return
}

// Node.

func (node *Node) SetId(id MCID_TYPE) {
	node.mcid = NewMasterCatalogId(id)
	return
}

func (node *Node) ReadCheckType(bfr *bytes.Buffer, reqtyp LBTYPE, nilok bool, desc string) (typ LBTYPE, err error) {
	err = node.debug.DecodeError(binary.Read(bfr, BIGEND, &typ))
    if err != nil {return}
	if typ == reqtyp || (nilok && typ == LBTYPE_NIL) {return}
	nilstr := ""
	if !nilok && typ == LBTYPE_NIL {
		nilstr = ", and this node does not permit nil type"
	}
	err = node.debug.Error(FmtErrBadType(
		"Decoding %s bytes, LBTYPE should be %v but is %v" + nilstr,
		desc, reqtyp, typ))
    return
}

func (node *Node) ReadSizedBytes(bfr *bytes.Buffer) (byts []byte, err error) {
	var size LBUINT
	err = node.debug.DecodeError(binary.Read(bfr, BIGEND, &size))
    if err != nil {return}
	byts = make([]byte, int(size))
	err = node.debug.DecodeError(binary.Read(bfr, BIGEND, &byts))
    return
}

// Read the parent MCID set, which is always the last section of the buffer.
func (node *Node) ReadMCIDSet(bfr *bytes.Buffer) (err error) {
	rem := bfr.Len() % int(MCID_TYPE_SIZE)
	if rem > 0 {
		err = node.debug.Error(FmtErrPartialMCIDSet(bfr.Len(), MCID_TYPE_SIZE))
		return
	}
	n := bfr.Len() / int(MCID_TYPE_SIZE)
	var id MCID_TYPE
	node.parents.set = make([]*MasterCatalogId, n)
	for i := 0; i < n; i++ {
		err = node.debug.Error(binary.Read(bfr, BIGEND, &id))
		node.parents.set[i] = NewMasterCatalogId(id)
		if err != nil {break}
	}
	return
}

// Return a byte slice with a Node packed ready for file writing.
func (node *Node) Pack() []byte {
	bfr := new(bytes.Buffer)
	// Write MCID
	binary.Write(bfr, BIGEND, LBTYPE_MCID)
	binary.Write(bfr, BIGEND, node.Id())
	// Write Master Catalog string key
	binary.Write(bfr, BIGEND, LBTYPE_MCK)
	ksz := AsLBUINT(len(node.Name()))
	binary.Write(bfr, BIGEND, ksz)
	bfr.Write([]byte(node.Name()))
	// Write field map
	if node.HasFields() > 0 {
		binary.Write(bfr, BIGEND, LBTYPE_MAP)
		byts := node.FieldMap.ToBytes(node.debug)
		binary.Write(bfr, BIGEND, AsLBUINT(len(byts)))
		bfr.Write(byts)
	} else {
		binary.Write(bfr, BIGEND, LBTYPE_NIL)
	}
	// Write parents MCID set
	if node.HasParents() > 0 {
		binary.Write(bfr, BIGEND, LBTYPE_MCID_SET)
		byts := node.parents.ToBytes(node.debug)
		binary.Write(bfr, BIGEND, AsLBUINT(len(byts)))
		bfr.Write(byts)
	} else {
		binary.Write(bfr, BIGEND, LBTYPE_NIL)
	}
	return bfr.Bytes()
}

// Unpack Node bytes.
func (node *Node) FromBytes(bfr *bytes.Buffer) (error) {
	// Read MCID
	_, err := node.ReadCheckType(bfr, LBTYPE_MCID, false, "mcid") // read LBTYPE
    if err != nil {return err}
	var id MCID_TYPE
	err = node.debug.Error(binary.Read(bfr, BIGEND, &id)) // read MCID
	node.SetId(id)
    if err != nil {return err}
	// Read Master Catalog string key
	_, err = node.ReadCheckType(bfr, LBTYPE_MCK, false, "name") // read LBTYPE
    if err != nil {return err}
	kbyts, err := node.ReadSizedBytes(bfr) // read name key
    if err != nil {return err}
	node.name = string(kbyts)
	// Read field map (optional or LBTYPE_NIL allowed in case of a LBTYPE_KIND)
	nilok := (node.ntype == LBTYPE_KIND)
	typ, err := node.ReadCheckType(bfr, LBTYPE_MAP, nilok, "field map") // read LBTYPE
    if err != nil {return err}
	node.FieldMap = NewFieldMap()
	if typ != LBTYPE_NIL {
		fbyts, err := node.ReadSizedBytes(bfr) // read field map bytes
		if err != nil {return err}
		node.FieldMap.FromBytes(bytes.NewBuffer(fbyts), node.debug)
	}
	// Read parents MCID set (can be LBTYPE_NIL)
	typ, err = node.ReadCheckType(bfr, LBTYPE_MCID_SET, true, "parents set") // read LBTYPE
    if err != nil {return err}
	if typ != LBTYPE_NIL {
		pbyts, err := node.ReadSizedBytes(bfr) // read field map bytes
		if err != nil {return err}
		err = node.parents.FromBytes(bytes.NewBuffer(pbyts), node.debug) // read parents set
		return err
	}
    return nil
}

func MintNode(ntype LBTYPE) *Node {
	return &Node{
		ntype:		ntype,
		parents:	NewMasterCatalogIdSet(),
		debug:		ScreenLogger,
		FieldMap:	NewFieldMap(),
	}
}

func MakeNode(name string, ntype LBTYPE, debug *DebugLogger) *Node {
	switch ntype {
    case LBTYPE_KIND, LBTYPE_DOC:
		return &Node{
			name:		name,
			ntype:		ntype,
			parents:	NewMasterCatalogIdSet(),
			debug:		debug,
			FieldMap:	NewFieldMap(),
		}
	default:
		debug.Error(FmtErrBadType("Bad key type: %d", ntype))
		return nil
	}
}

func NormaliseNodeName(name string, ntype LBTYPE) string {
	prefix := NodeConfigs[ntype].namespace + ":"
    base := strings.TrimPrefix(name, prefix)
	return prefix + base
}

func (lbase *Logbase) NewNode(name string, ntype LBTYPE) (node *Node, exists bool, err error) {
	name = NormaliseNodeName(name, ntype)
	vbyts, vtype, err := lbase.Get(name)
	if err != nil {return}
	node = MakeNode(name, ntype, lbase.debug)
	if vbyts == nil {
		exists = false
		// A new node
		node.SetId(GetAndIncNextMCID())
	} else {
		exists = true
		// Read existing node data
		if vtype != LBTYPE_MCID {
			err = lbase.debug.Error(FmtErrBadType(
				"Found record in logbase %s for node %q with type %v, " +
				"but should be type %v",
				lbase.name, name, vtype, LBTYPE_MCID))
			return
		}
		id, err1 := BYTES(vbyts).ToIdKey(lbase.debug)
		if err1 != nil {err = err1; return}
		vbyts, vtype, err = lbase.Get(id)
		if vbyts == nil {
			err = lbase.debug.Error(FmtErrKeyNotFound(id))
			return
		}
		if vtype != ntype {
			err = lbase.debug.Error(FmtErrBadType(
				"Found record %v in logbase %s for node %q via MCID %v " +
				"with type %v, but should be type %v",
				vbyts, lbase.name, name, id, vtype, ntype))
			return
		}
		node.FromBytes(bytes.NewBuffer(vbyts))
	}
	return
}

// Save two records, the first maps the node MCID to its complete binary
// representation, the second maps the name string to the parents set.
func (node *Node) Save(lbase *Logbase) error {
	lbase.debug.Basic("Saving %q to logbase %s", node.Name(), lbase.Name())
	_, err := lbase.Put(node.MCID().id, node.Pack(), LBTYPE_KIND)
	if node.debug.Error(err) != nil {return err}
	_, err = lbase.Put(node.Name(), node.MCID().ToBytes(node.debug), LBTYPE_MCID)
	return node.debug.Error(err)
}

func (node *Node) String() string {
	return fmt.Sprintf("{%q %v %v %v}",
		node.Name(),
		node.MCID(),
		node.GetFieldMap(),
		node.Parents())
}

func (node *Node) MCID() *MasterCatalogId {return node.mcid}
func (node *Node) Id() MCID_TYPE {return node.mcid.id}
func (node *Node) NodeType() LBTYPE {return node.ntype}
func (node *Node) Name() string {return node.name}
func (node *Node) Fields() map[string]*Field {return node.FieldMap.fields}
func (node *Node) GetFieldMap() *FieldMap {return node.FieldMap}
func (node *Node) Parents() *MasterCatalogIdSet {return node.parents}

// A Node can only have parents of type Kind.
func (node *Node) AddParent(parent *Node) *Node {
	if parent.NodeType() != LBTYPE_KIND {
		node.debug.Error(FmtErrBadType(
			"Only a node of LBTYPE_KIND can be a parent, nothing done"))
	} else {
		node.parents.Add(parent.MCID())
	}
	return node
}

// Alias for AddParent.
func (node *Node) OfKind(parent *Node) *Node {
    return node.AddParent(parent)
}

func (node *Node) HasParent(parent *Node) bool {
	return node.Parents().Contains(parent.MCID())
}

func (node *Node) HasParents() int {
	return len(node.Parents().set)
}

func (node *Node) HasFields() int {
	return len(node.Fields())
}

// Kind.

// Create a Kind instance, and retrieve the logbase data for it if it exists.
func (lbase *Logbase) Kind(name string) (*Node, bool, error) {
	return lbase.NewNode(name, LBTYPE_KIND)
}

// Document.

// Create a Document instance, and retrieve the logbase data for it if it exists.
func (lbase *Logbase) Doc(name string) (*Node, bool, error) {
	return lbase.NewNode(name, LBTYPE_DOC)
}

// Fields.

func NewField() *Field {
	return &Field{
		Vtype: &Vtype{},
		Vdata: &Vdata{},
	}
}

func MakeField(vbyts []byte, vtype LBTYPE) *Field {
	return &Field{
		Vtype: &Vtype{vtype},
		Vdata: &Vdata{vbyts},
	}
}

func NewFieldMap() *FieldMap {
	return &FieldMap{
		fields: make(map[string]*Field),
	}
}

func (node *Node) SetFieldWithType(label string, val interface{}, vtype LBTYPE) *Node {
	vbyts, err := ToBytes(val, vtype, node.debug)
	if err != nil {
		node.debug.Error(FmtErrBadType(
			"Attempt to set field %q to %v using LBTYPE %v for node %q, " +
			"nothing done",
			label, val, vtype, node.Name()))
		return node
	}
	fold, exists := node.fields[label]
	fnew := MakeField(vbyts, vtype)
	if exists {
		node.debug.Fine("Replacing old field %v with %v", fold, fnew)
	} else {
		node.debug.Fine("Creating new field %v", fnew)
	}
	node.fields[label] = fnew
	return node
}

// 
func (fmap *FieldMap) ToBytes(debug *DebugLogger) []byte {
	bfr := new(bytes.Buffer)
	var vsz LBUINT
	var lbyts []byte
	for label, field := range fmap.fields {
		lbyts = []byte(label)
		binary.Write(bfr, BIGEND, AsLBUINT(len(lbyts))) // label size
		bfr.Write(lbyts) // label
		vsz = AsLBUINT(len(field.vbyts)) + LBUINT(LBTYPE_SIZE)
		binary.Write(bfr, BIGEND, vsz) // value size, including LBTYPE
		binary.Write(bfr, BIGEND, field.vtype) // LBTYPE
		bfr.Write(field.vbyts) // value
	}
	return bfr.Bytes()
}

func (fmap *FieldMap) FromBytes(bfr *bytes.Buffer, debug *DebugLogger) (err error) {
	var size LBUINT
	var label string
	var vtype LBTYPE
	var bits []byte
	for bfr.Len() > 0 {
		err = debug.DecodeError(binary.Read(bfr, BIGEND, &size)) // label size
		if err == io.EOF {break} else {if err != nil {return}}
		bits = make([]byte, int(size))
		err = debug.DecodeError(binary.Read(bfr, BIGEND, &bits))
		if err == io.EOF {break} else {if err != nil {return}}
		label = string(bits) // label
		err = debug.DecodeError(binary.Read(bfr, BIGEND, &size)) // value size
		if err == io.EOF {break} else {if err != nil {return}}
		bits = make([]byte, int(size) - LBTYPE_SIZE)
		err = debug.DecodeError(binary.Read(bfr, BIGEND, &vtype)) // LBTYPE
		if err == io.EOF {break} else {if err != nil {return}}
		err = debug.DecodeError(binary.Read(bfr, BIGEND, &bits))
		fmap.fields[label] = MakeField(bits, vtype)
		if err == io.EOF {break} else {if err != nil {return}}
	}
	return
}

func (field *Field) String() string {
	return fmt.Sprintf("%d,%s",
		field.vtype,
		ValBytesToString(field.vbyts, field.vtype))
}

func (fmap *FieldMap) String() string {
	result := "{"
	first := true
	for label, field := range fmap.fields {
		if !first {result += ","; first = false}
		result += fmt.Sprintf("%q:(%s)", label, field.String())
	}
	return result + "}"
}

// Documents.

//func NewDoc(name string, kind *Node) *Document {
//	return &Document{
//		name: name,
//		kind: kind,
//		FieldMap: NewFieldMap(),
//	}
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

// Get and increment next MCID counter value.
func GetAndIncNextMCID() MCID_TYPE {
	n := nextMCID
	IncMCID()
	return n
}

// Test whether the given key value is of type MCID_TYPE.
func IsMCID(key interface{}) bool {
	_, isMCID := key.(MCID_TYPE)
	if isMCID {return true}
	return false
}

// If the given key value is of the correct type, increment the MCID counter.
func SetNextMCID(key interface{}) {
	if mcid, isMCID := key.(MCID_TYPE); isMCID {
		nextMCID = mcid + 1
	}
	return
}

func (lbase *Logbase) InitDocCat() {
	return
}
