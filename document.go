/*
	Defines and manages documents, which are indexed sets of k-v pairs and other documents.

	A Document is a collection of labelled fields mapped to FieldRefs, which can be either
	a key in the Master Catalog, or another Document.

	DocCat
    +-----------+
    | "Animal" -------------+ 
	+-----------+           |
	| "Mineral" |           V  DocMap
	+-----------+    +--------------+
	| "Plant"   |    | string(frog) --------------+
	+-----------+    +--------------+             |
	:			:    |   uint8(34)  |             V  Doc
					 +--------------+      +----------------+--------------------------------+
					 | string(Pete) |      | string(colour) | MCR:string(Animal.frog.colour) |
					 +--------------+      +----------------+--------------------------------+
					 :              :      | string(type)   | DOC:"Animal"string(amphibian)  |
                                           +----------------+--------------------------------+
                                           :                :                                :
*/
package logbase

import (
	"sync"
)

type FieldRef struct {
	reftype	LBTYPE // Must be either LBTYPE_MCR or LBTYPE_DOC
	docref	*DocRef
}

type DocRef struct {
	kind	string // Empty if DocRef is an MCR key
	key		interface{}
}

func NewDocRef() *DocRef {
	return &DocRef{}
}

type Document struct {
	*DocRef // Key is embedded in the Document itself
	fields	map[interface{}]*FieldRef
}

func NewDocument() *Document {
	return &Document{
		DocRef: NewDocRef(),
		fields: make(map[interface{}]*FieldRef),
	}
}

type DocumentRecord struct {
	*Document
	sync.RWMutex
}

func NewDocumentRecord() *DocumentRecord {
	return &DocumentRecord{
		Document: NewDocument(),
	}
}

// Maps document instances to field maps.
type DocumentMap struct {
	index   map[interface{}]*DocumentRecord
	file    *DocKindFile
	sync.RWMutex
	changed	bool // Has index changed since last save?
}

func NewDocumentMap() *DocumentMap {
	return &DocumentMap{
		index: make(map[interface{}]*DocumentRecord),
	}
}

// Allow persistence of Document Catalog.
type DocKindFile struct {
	*File
}

// Init a DocKindFile.
func NewDocKindFile(file *File) *DocKindFile {
	return &DocKindFile{
		File: file,
	}
}

//  Catalog of all documents.
type DocumentCatalog struct {
	kinds   map[string]*DocumentMap
	sync.RWMutex
}

// Init a Document Catalog.
func NewDocumentCatalog() *DocumentCatalog {
	return &DocumentCatalog{
		kinds: make(map[string]*DocumentMap),
	}
}

