Usage: First call a NewFvnKeyIter method to form iterators. Then call NewHashmap on the iterator.

size corresponds to an entry with an individual key. depth is how many actual data points each entry has.
Higher size = better concurrency, higher depth = increased robustness. Both come at the cost of increased
memory allocation.

keyLen is the byte length of the keys that will be used with the map. Supported values are 32 for hashes
and 20 for addresses.

stepSize of NewFnvKeyIterWithIndexSkip decreases the computation time of each hash lookup at the cost of
increased hash collision (thus lower robustness)