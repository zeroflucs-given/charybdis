package generics

// Numeric is a type that contains all the available numeric types known to go
type Numeric interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~int | ~int8 | ~int16 | ~int32 | ~int64 | ~float32 | ~float64
}

// Comparable is the set of comparable types for our operators
type Comparable interface {
	Numeric | ~string
}
