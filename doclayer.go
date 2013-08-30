/*
	Defines and manages Nodes, which can include Kinds or Documents.

	colour := Kind("Colour")
	// Doesn't exist, CATID_MIN = 10
	// LB+ LBTYPE_CATID(10) -> LBTYPE_KIND(LBTYPE_CATKEY("Colour"),LBTYPE_CATID_SET(10))
	// LB+ "Colour" -> LBTYPE_CATID(10)
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
	// LB+ LBTYPE_CATID(11) -> LBTYPE_CATKEY("/Thing/Animal/")
	// LB+ "/Thing/Animal/" -> LBTYPE_CATID(11)
	// LB+ LBTYPE_CATIDS(12) -> VALOC -> LBTYPE_DOC(LBTYPE_CATKEY("Thing/Animal/frog"),LBTYPE_MAP("colour":LBTYPE_CATIDS(1),"eyes":LBTYPE_UINT8(2)))
	// LB+ "/Thing/Animal/frog" -> LBTYPE_CATIDS(12)

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

const (
	NODE_TYPE_SEPARATOR string = ":"
)

type NodeConfig struct {
	namespace	string
}

var NodeConfigs = map[LBTYPE]*NodeConfig{
	LBTYPE_KIND:	&NodeConfig{"kind"},
	LBTYPE_DOC:		&NodeConfig{"doc"},
}

var NodeKeyPrefix map[string]LBTYPE = make(map[string]LBTYPE)

func init() {
	// Create namespace reverse map, adding in NODE_TYPE_SEPARATOR
	for typ, ncfg := range NodeConfigs {
		NodeKeyPrefix[ncfg.namespace + NODE_TYPE_SEPARATOR] = typ
	}
}

// A Node can represent a "kind" (type or class) or a "document".
type Node struct {
	cid			*CatalogId
	name		string // MC string key stored with namespace prefix
	parents		*CatalogIdSet
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

// Node.

func (node *Node) SetId(id CATID_TYPE) {
	node.cid = NewCatalogId(id)
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

// Read the parent CATID set, which is always the last section of the buffer.
func (node *Node) ReadCATIDSet(bfr *bytes.Buffer) (err error) {
	rem := bfr.Len() % int(CATID_TYPE_SIZE)
	if rem > 0 {
		err = node.debug.Error(FmtErrPartialCATIDSet(bfr.Len(), CATID_TYPE_SIZE))
		return
	}
	n := bfr.Len() / int(CATID_TYPE_SIZE)
	var id CATID_TYPE
	node.parents.set = make([]*CatalogId, n)
	for i := 0; i < n; i++ {
		err = node.debug.Error(binary.Read(bfr, BIGEND, &id))
		node.parents.set[i] = NewCatalogId(id)
		if err != nil {break}
	}
	return
}

// Return a byte slice with a Node packed ready for file writing.
func (node *Node) Pack() []byte {
	bfr := new(bytes.Buffer)
	// Write CATID
	binary.Write(bfr, BIGEND, LBTYPE_CATID)
	binary.Write(bfr, BIGEND, node.Id())
	// Write  Catalog string key
	binary.Write(bfr, BIGEND, LBTYPE_CATKEY)
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
	// Write parents CATID set
	if node.HasParents() > 0 {
		binary.Write(bfr, BIGEND, LBTYPE_CATID_SET)
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
	// Read CATID
	_, err := node.ReadCheckType(bfr, LBTYPE_CATID, false, "cid") // read LBTYPE
    if err != nil {return err}
	var id CATID_TYPE
	err = node.debug.Error(binary.Read(bfr, BIGEND, &id)) // read CATID
	node.SetId(id)
    if err != nil {return err}
	// Read  Catalog string key
	_, err = node.ReadCheckType(bfr, LBTYPE_CATKEY, false, "name") // read LBTYPE
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
	// Read parents CATID set (can be LBTYPE_NIL)
	typ, err = node.ReadCheckType(bfr, LBTYPE_CATID_SET, true, "parents set") // read LBTYPE
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
		parents:	NewCatalogIdSet(),
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
			parents:	NewCatalogIdSet(),
			debug:		debug,
			FieldMap:	NewFieldMap(),
		}
	default:
		debug.Error(FmtErrBadType("Bad key type: %d", ntype))
		return nil
	}
}

func NormaliseNodeName(name string, ntype LBTYPE) string {
	prefix := NodeConfigs[ntype].namespace + NODE_TYPE_SEPARATOR
    basename := strings.TrimPrefix(name, prefix)
	return prefix + basename
}

func (lbase *Logbase) NewNode(name string, ntype LBTYPE, create bool) (node *Node, exists bool, err error) {
	name = NormaliseNodeName(name, ntype)
	vbyts, vtype, err := lbase.Get(name)
	if err != nil {return}
	exists = false
	if vbyts == nil && create {
		// A new node
		node = MakeNode(name, ntype, lbase.debug)
		node.SetId(lbase.MasterCatalog().PopNextId())
	} else {
		exists = true
		node = MakeNode(name, ntype, lbase.debug)
		// Read existing node data
		if vtype != LBTYPE_CATID {
			err = lbase.debug.Error(FmtErrBadType(
				"Found record in logbase %s for node %q with type %v, " +
				"but should be type %v",
				lbase.name, name, vtype, LBTYPE_CATID))
			return
		}
		id, err1 := BytesToCatalogId(vbyts, lbase.debug)
		if err1 != nil {err = err1; return}
		vbyts, vtype, err = lbase.Get(id)
		if vbyts == nil {
			err = lbase.debug.Error(FmtErrKeyNotFound(id))
			return
		}
		if vtype != ntype {
			err = lbase.debug.Error(FmtErrBadType(
				"Found record %v in logbase %s for node %q via CATID %v " +
				"with type %v, but should be type %v",
				vbyts, lbase.name, name, id, vtype, ntype))
			return
		}
		node.FromBytes(bytes.NewBuffer(vbyts))
	}
	return
}

// Save two records, the first maps the node CATID to its complete binary
// representation, the second maps the name string to the parents set.
func (node *Node) Save(lbase *Logbase) error {
	lbase.debug.Basic("Saving %q to logbase %s", node.Name(), lbase.Name())
	_, err := lbase.Put(node.CATID().id, node.Pack(), LBTYPE_KIND)
	if node.debug.Error(err) != nil {return err}
	_, err = lbase.Put(node.Name(), node.CATID().ToBytes(node.debug), LBTYPE_CATID)
	return node.debug.Error(err)
}

func (node *Node) String() string {
	return fmt.Sprintf("{%q %v %v %v}",
		node.Name(),
		node.CATID(),
		node.GetFieldMap(),
		node.Parents())
}

// Getters.

func (node *Node) CATID() *CatalogId {return node.cid}
func (node *Node) Id() CATID_TYPE {return node.cid.id}
func (node *Node) NodeType() LBTYPE {return node.ntype}
func (node *Node) Name() string {return node.name}
func (node *Node) Fields() map[string]*Field {return node.FieldMap.fields}
func (node *Node) GetFieldMap() *FieldMap {return node.FieldMap}
func (node *Node) Parents() *CatalogIdSet {return node.parents}

// A Node can only have parents of type Kind.
func (node *Node) AddParent(parent *Node) *Node {
	if parent.NodeType() != LBTYPE_KIND {
		node.debug.Error(FmtErrBadType(
			"Only a node of LBTYPE_KIND can be a parent, nothing done"))
	} else {
		node.parents.Add(parent.CATID())
	}
	return node
}

// Alias for AddParent.
func (node *Node) OfKind(parent *Node) *Node {
    return node.AddParent(parent)
}

func (node *Node) HasParent(parent *Node) bool {
	return node.Parents().Contains(parent.CATID())
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
	return lbase.NewNode(name, LBTYPE_KIND, true)
}

// Retrieve the Kind if it exists.
func (lbase *Logbase) GetKind(name string) (*Node, bool, error) {
	return lbase.NewNode(name, LBTYPE_KIND, false)
}

// Document.

// Create a Document instance, and retrieve the logbase data for it if it exists.
func (lbase *Logbase) Doc(name string) (*Node, bool, error) {
	return lbase.NewNode(name, LBTYPE_DOC, true)
}

// Retrieve the Doc if it exists.
func (lbase *Logbase) GetDoc(name string) (*Node, bool, error) {
	return lbase.NewNode(name, LBTYPE_DOC, false)
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

// Find.

func GetNodeNameType(key interface{}) (string, LBTYPE) {
	if k, ok := key.(string); ok {
		for prefix, ntype := range NodeKeyPrefix { // note prefix includes separator
			if strings.HasPrefix(k, prefix) {
				basename := strings.TrimPrefix(k, prefix)
				return basename, ntype
			}
		}
	}
	return "", LBTYPE_NIL
}

func (lbase *Logbase) FindOfKind(name string, ntype LBTYPE) []*Node {
	var result []*Node
	kind, exists, err := lbase.NewNode(name, LBTYPE_KIND, false)
	if err != nil {
		lbase.debug.Error(err)
		return nil
	}
	if !exists {
		lbase.debug.Error(
			FmtErrKeyNotFound(NormaliseNodeName(name, LBTYPE_KIND)))
		return nil
	}
	var basename string
	var typ LBTYPE
	for key, _ := range lbase.mcat.index {
        basename, typ = GetNodeNameType(key)
		if typ == ntype {
			node, _, err := lbase.NewNode(basename, ntype, true)
			lbase.debug.Error(err)
			if err == nil && node.Parents().Contains(kind.CATID()) {
				result = append(result, node)
			}
		}
	}
	return result
}

func (lbase *Logbase) FindKindOfKind(name string) []*Node {
	return lbase.FindOfKind(name, LBTYPE_KIND)
}

func (lbase *Logbase) FindDocOfKind(name string) []*Node {
	return lbase.FindOfKind(name, LBTYPE_DOC)
}
