package concurrent_hashmap

import (
	"errors"
	"fmt"
	"sync"
)

const (
	KeyLen32   = 32
	KeyLen20   = 20
	KeyLenUuid = 16
)

type Hashmap struct {
	data     []*value
	hashFunc func([]byte) int
	depth    int
	keyLen   int
}

type value struct {
	lock  *sync.RWMutex
	data  map[interface{}]interface{}
	pIdx  int
	uData []interface{}
}

func makeValue(depth int) *value {
	data := make(map[interface{}]interface{}, depth)
	uData := make([]interface{}, depth)
	var lock sync.RWMutex
	pIdx := 0

	return &value{
		lock:  &lock,
		pIdx:  pIdx,
		data:  data,
		uData: uData,
	}
}

func (h *Hashmap) keyToFixedArray(key []byte) interface{} {
	switch h.keyLen {
	case KeyLen20:
		return *(*[KeyLen20]byte)(key)
	case KeyLen32:
		return *(*[KeyLen32]byte)(key)
	default:
		return nil
	}
}

// NewHashmap
// depth is the entries each shard will have. keyLen should be 32 for tx hashes, 20 for addresses
func (iter FnvKeyIter) NewHashmap(size int, depth int, keyLen int) (*Hashmap, error) {
	switch keyLen {
	case KeyLen20:
	case KeyLen32:
	case KeyLenUuid:
	default:
		return nil, errors.New("keyLen not supported")
	}
	fnv := iter.newFnv32a(size)
	values := make([]*value, fnv.domainSize)
	for i := 0; i < fnv.domainSize; i++ {
		values[i] = makeValue(depth)
	}
	return &Hashmap{
		data:     values,
		hashFunc: fnv.hashF,
		depth:    depth,
		keyLen:   keyLen,
	}, nil
}

func (h *Hashmap) ContainsWithAdd(key []byte) (bool, error) {
	_, contains, err := h.get(key, true, false, false, nil)
	if err != nil {
		return false, err
	}

	return contains, nil
}

func (h *Hashmap) Contains(key []byte) (bool, error) {
	_, contains, err := h.get(key, nil, false, true, nil)
	if err != nil {
		return false, err
	}

	return contains, nil
}

func (h *Hashmap) Get(key []byte) (interface{}, error) {
	ret, _, err := h.get(key, nil, false, true, nil)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

// GetAndSwap will remove if value is nil. DO NOT use it to store nil values
func (h *Hashmap) GetAndSwap(key []byte, value interface{}) (interface{}, error) {
	ret, _, err := h.get(key, value, value == nil, false, nil)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

// GetAndSwapConditional is like GetAndSwap except for when the value already exists. Then it will swap only if cond is true.
// CAN NOT be used for removing values for now.
func (h *Hashmap) GetAndSwapConditional(key []byte, value interface{}, cond func(val interface{}) bool) (interface{}, error) {
	ret, _, err := h.get(key, value, value == nil, false, cond)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

// get will add if value is not nil
func (h *Hashmap) get(key []byte, value interface{}, remove bool, readOnly bool, cond func(val interface{}) bool) (interface{}, bool, error) {
	if len(key) != h.keyLen {
		return nil, false, errors.New(fmt.Sprintf("length of key bytes should be %d", h.keyLen))
	}

	hash := h.hashFunc(key)
	if readOnly {
		h.data[hash].lock.RLock()
		defer h.data[hash].lock.RUnlock()
	} else {
		h.data[hash].lock.Lock()
		defer h.data[hash].lock.Unlock()
	}

	val := h.data[hash]

	keyFixed := h.keyToFixedArray(key)
	oldVal, contains := val.data[keyFixed]
	if remove {
		delete(val.data, keyFixed)
	}
	if value == nil {
		return oldVal, contains, nil
	}

	if contains {
		swap := false
		if cond == nil {
			swap = true
		} else {
			swap = cond(oldVal)
		}
		if swap {
			val.data[keyFixed] = value
		}
	} else {
		delete(val.data, val.uData[val.pIdx])
		val.uData[val.pIdx] = keyFixed
		val.data[keyFixed] = value
		val.pIdx = (val.pIdx + 1) % h.depth
	}

	return oldVal, contains, nil
}

func (h *Hashmap) GetWithUuid(key [16]byte) (interface{}, error) {
	ret, _, err := h.getWithUuid(key, nil, false, true)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

// GetAndSwapWithUuid will remove if value is nil. DO NOT use it to store nil values
func (h *Hashmap) GetAndSwapWithUuid(key [16]byte, value interface{}) (interface{}, error) {
	ret, _, err := h.getWithUuid(key, value, value == nil, false)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (h *Hashmap) getWithUuid(key [16]byte, value interface{}, remove bool, readOnly bool) (interface{}, bool, error) {
	if h.keyLen != KeyLenUuid {
		return nil, false, errors.New("hashmap key type is not uuid")
	}
	hash := h.hashFunc(key[:])
	if readOnly {
		h.data[hash].lock.RLock()
		defer h.data[hash].lock.RUnlock()
	} else {
		h.data[hash].lock.Lock()
		defer h.data[hash].lock.Unlock()
	}
	val := h.data[hash]

	oldVal, contains := val.data[key]
	if remove {
		delete(val.data, key)
	}
	if value == nil {
		return oldVal, contains, nil
	}

	if contains {
		val.data[key] = value
	} else {
		delete(val.data, val.uData[val.pIdx])
		val.uData[val.pIdx] = key
		val.data[key] = value
		val.pIdx = (val.pIdx + 1) % h.depth
	}

	return oldVal, contains, nil
}
