package concurrent_hashmap

const prime32 = uint32(16777619)
const initOffset = uint32(2166136261)

func defGet(key []byte, i int) byte {
	return key[i]
}

func defRange(key []byte) int {
	return len(key)
}

type FnvKeyIter struct {
	get   func([]byte, int) byte
	len   func([]byte) int
	clamp func(uint32) int
}

type fnv struct {
	domainSize int
	hashF      func([]byte) int
}

// NewFvnKeyIter iterGet and iterRange can be nil for default ranging over all key values.
func NewFvnKeyIter(iterGet func([]byte, int) byte, iterRange func([]byte) int) FnvKeyIter {
	if iterGet == nil {
		iterGet = defGet
	}
	if iterRange == nil {
		iterRange = defRange
	}
	return FnvKeyIter{
		get: iterGet,
		len: iterRange,
	}
}

// NewFnvKeyIterWithIndexSkip generates a FvnKeyIter that skips over the elements of keys with the given stepSize.
// e.g. [1, 2, 3, 4, 5, 6, 7, 8] with stepSize=3 would iter over values [1, 4, 7]. This is useful for reducing
// computation time of the fvn hash function as it loops over all the elements of a key at the cost of reduced uniqueness.
// To make up for the increased hash collision use a higher depth for the resulting hashmap.
func NewFnvKeyIterWithIndexSkip(stepSize int) FnvKeyIter {
	return FnvKeyIter{
		get: func(key []byte, i int) byte {
			return key[i*stepSize]
		},
		len: func(key []byte) int {
			l := len(key) / stepSize
			if len(key)%stepSize != 0 {
				l++
			}
			return l
		},
	}
}

// newFnv32a
// domainSize can be -1 for default u32bit domain max. Domain range is [0, domainSize)
func (iter FnvKeyIter) newFnv32a(domainSize int) fnv {
	clamp := func(i uint32) int {
		return int(i)
	}
	if domainSize > 1 {
		clamp = func(i uint32) int {
			return int(i) % domainSize
		}
	}
	iter.clamp = clamp
	hashF := iter.fnv32a

	return fnv{
		domainSize: domainSize,
		hashF:      hashF,
	}
}

func (iter FnvKeyIter) fnv32a(key []byte) int {
	hash := initOffset
	keyLength := iter.len(key)
	for i := 0; i < keyLength; i++ {
		hash ^= uint32(iter.get(key, i))
		hash *= prime32
	}
	return iter.clamp(hash)
}
