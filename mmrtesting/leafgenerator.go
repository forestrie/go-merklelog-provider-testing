package mmrtesting

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/forestrie/go-merklelog/massifs"
	"github.com/forestrie/go-merklelog/massifs/storage"
)

// AddLeafArgs is the return value of all LeafGenerator implementations. It
// carries just enough to successfully call AddLeafEntry on a MassifContext The
// leaf generator is free to decide on appropriate values
type AddLeafArgs struct {
	ID    uint64
	AppID []byte
	Value []byte
	LogID []byte
}

type LeafGenerator struct {
	// base uint64,
	// count uint64,
	Generator LeafContentGenerator
	Encoder   AddLeafArgsEncoder
}

type GeneratedLeaves struct {
	Encoded     []any
	Args        []AddLeafArgs
	LeafIndices []uint64
	MMRIndices  []uint64
}

// AddLeafArgsGenerator is a function that generates AddLeafArgs
type AddLeafArgsGenerator func(logID storage.LogID, base, i uint64) AddLeafArgs
type LeafContentGenerator func(logID storage.LogID, base, i uint64) any
type AddLeafArgsEncoder func(a any) AddLeafArgs

func NewLeafGenerator(gen LeafContentGenerator, enc AddLeafArgsEncoder) LeafGenerator {
	return LeafGenerator{
		Generator: gen,
		Encoder:   enc,
	}
}

func (g *LeafGenerator) Generate(logID storage.LogID, base, i uint64) (AddLeafArgs, any) {
	content := g.Generator(logID, base, i)
	addArgs := g.Encoder(content)
	return addArgs, content
}

func MMRTestingGenerateNumberedLeaf(logID storage.LogID, base, i uint64) AddLeafArgs {
	h := sha256.New()
	HashWriteUint64(h, base+i)

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, base+i)
	return AddLeafArgs{
		ID:    0,
		AppID: b,
		Value: h.Sum(nil),
	}
}

func GenerateAddLeafArgsBatch(
	g *TestGenerator, logID storage.LogID, base, count uint64, leafGenerator AddLeafArgsGenerator) []AddLeafArgs {
	leaves := make([]AddLeafArgs, 0, count)
	for i := range count {
		args := leafGenerator(logID, base, i)
		leaves = append(leaves, args)
	}
	return leaves
}

func GenerateLeafContentBatch(
	g *TestGenerator, logID storage.LogID, base, count uint64, leafGenerator LeafContentGenerator) []any {
	contents := make([]any, 0, count)
	for i := range count {
		content := leafGenerator(logID, base, i)
		contents = append(contents, content)
	}
	return contents
}

func PadWithNumberedLeaves(data []byte, first, n int) []byte {
	if n == 0 {
		return data
	}
	values := make([]byte, massifs.ValueBytes*n)
	for i := range n {
		binary.BigEndian.PutUint32(values[i*massifs.ValueBytes+massifs.ValueBytes-4:i*massifs.ValueBytes+massifs.ValueBytes], uint32(first+i))
	}
	return append(data, values...)
}
