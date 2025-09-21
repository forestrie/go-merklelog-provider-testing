package mmrtesting

import (
	"encoding/binary"
)

type HashOption func(any)

type HashOptions struct {
	AccumulateHash bool
	Prefix         []byte
	Committed      uint64
	IDCommitted    []byte
}

type LeafHasher interface {
	Reset()
	Sum(b []byte) []byte
	GetAppID() []byte
	HashLeafContent(content any, opts ...HashOption) error
}

// WithIDCommitted includes the snowflakeid unique commitment timestamp in the hash
// idcommitted is never (legitimately) zero
func WithIDCommitted(idcommitted uint64) HashOption {
	return func(a any) {
		o, ok := a.(*HashOptions)
		if !ok || o == nil {
			return
		}
		o.Committed = idcommitted
		o.IDCommitted = make([]byte, 8)
		binary.BigEndian.PutUint64(o.IDCommitted, o.Committed)
	}
}

// WithPrefix pre-pends the provided bytes to the hash. This option can be used
// multiple times and the successive bytes are appended to the prefix. This is
// typically used to provide hash domain seperation where second pre-image
// collisions are a concerne.
func WithPrefix(b []byte) HashOption {
	return func(a any) {
		o, ok := a.(*HashOptions)
		if !ok || o == nil {
			return
		}
		o.Prefix = append(o.Prefix, b...)
	}
}

func WithAccumulate() HashOption {
	return func(a any) {
		o, ok := a.(*HashOptions)
		if !ok || o == nil {
			return
		}
		o.AccumulateHash = true
	}
}
