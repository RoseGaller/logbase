/*
	Defines and manages object caches.  Not only does an object cache
	save resources but we can keep a single RWMutex associated with
	each object.
*/
package logbase

import (
	"fmt"
)

type Cache struct {
	objects	map[interface{}]interface{}
}

// Init new file register.
func NewCache() *Cache {
	return &Cache{
		objects: make(map[interface{}]interface{}),
	}
}

func (cache *Cache) Put(key, obj interface{}) (interface{}, bool) {
    old, exists := cache.objects[key]
	cache.objects[key] = obj
	return old, exists
}

func (cache *Cache) Get(key interface{}) (interface{}, bool) {
    obj, exists := cache.objects[key]
	return obj, exists
}

func (cache *Cache) StringArray() []string {
	var result []string
	for k, _ := range cache.objects {
		result = append(result, fmt.Sprintf("%v", cache.objects[k]))
	}
	return result
}

