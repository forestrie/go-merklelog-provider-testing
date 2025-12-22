package providers

import (
	"bytes"
	"crypto/sha256"

	"github.com/forestrie/go-merklelog-provider-testing/mmrtesting"
	"github.com/forestrie/go-merklelog/bloom"
	"github.com/forestrie/go-merklelog/massifs"
	"github.com/forestrie/go-merklelog/urkle"
	"github.com/stretchr/testify/require"
)

// StorageMassifV2IndexRoundTripTest exercises the v2 index format (Bloom + Urkle)
// against a concrete storage provider.
//
// This is intended to provide integration coverage beyond the unit tests in
// go-merklelog/massifs + go-merklelog/urkle by asserting that:
//   - the bloom region is initialized and updated
//   - the urkle leaf table is populated
//   - extra1 is truncated to 24B in the leaf record ("Option B") and padded on read
func StorageMassifV2IndexRoundTripTest(tc mmrtesting.ProviderTestContext, factory BuilderFactory) {
	t := tc.GetT()
	ctx := t.Context()

	logID := tc.GetG().NewLogID()

	// Keep this small so it stays fast for provider-backed tests.
	const massifHeight = uint8(3) // leaf capacity N = 2^(h-1) = 4
	const epoch = uint32(1)

	builder := factory(massifHeight)
	builder.DeleteLog(logID)
	builder.SelectLog(ctx, logID)

	mc, err := massifs.GetAppendContext(ctx, builder.ObjectReader, epoch, massifHeight)
	require.NoError(t, err)

	// MMR leaf hash is independent of the v2 index `valueBytes`.
	leafHash := sha256.Sum256([]byte("mmr-leaf-0"))
	_, err = mc.AddIndexedEntry(leafHash[:])
	require.NoError(t, err)

	// v2 index key+valueBytes.
	idTimestamp := uint64(1)
	valueBytes := sha256.Sum256([]byte("value-bytes-0"))

	// Provide 3 stored extras. extra1 will be truncated to 24B in the on-disk leaf record.
	extra1 := bytes.Repeat([]byte{0xA1}, massifs.ValueBytes)
	extra2 := bytes.Repeat([]byte{0xB2}, massifs.ValueBytes)
	extra3 := bytes.Repeat([]byte{0xC3}, massifs.ValueBytes)

	// nil in extraData[0] means bloom0 indexes valueBytes.
	err = mc.IndexLeaf(idTimestamp, valueBytes[:], nil, extra1, extra2, extra3)
	require.NoError(t, err)
	mc.SetLastIDTimestamp(idTimestamp)

	require.NoError(t, massifs.CommitContext(ctx, builder.ObjectWriter, &mc))

	// Read back and validate index material.
	mc2, err := massifs.GetMassifContext(ctx, builder.ObjectReader, 0)
	require.NoError(t, err)

	region, err := mc2.BloomRegion()
	require.NoError(t, err)
	_, ok, err := bloom.DecodeHeaderV1(region)
	require.NoError(t, err)
	require.True(t, ok)

	ok0, err := bloom.MaybeContainsV1(region, 0, valueBytes[:])
	require.NoError(t, err)
	require.True(t, ok0)
	ok1, err := bloom.MaybeContainsV1(region, 1, extra1)
	require.NoError(t, err)
	require.True(t, ok1)
	ok2, err := bloom.MaybeContainsV1(region, 2, extra2)
	require.NoError(t, err)
	require.True(t, ok2)
	ok3, err := bloom.MaybeContainsV1(region, 3, extra3)
	require.NoError(t, err)
	require.True(t, ok3)

	leafTable, err := mc2.UrkleLeafTableRegion()
	require.NoError(t, err)
	require.Equal(t, idTimestamp, urkle.LeafKey(leafTable, 0))
	require.Equal(t, valueBytes, urkle.LeafValue(leafTable, 0))

	gotExtra1 := urkle.LeafExtra(leafTable, 0, 0)
	require.Equal(t, extra1[:24], gotExtra1[:24])
	require.True(t, bytes.Equal(make([]byte, 8), gotExtra1[24:]), "extra1 tail must be zero-padded on read")

	gotExtra2 := urkle.LeafExtra(leafTable, 0, 1)
	require.Equal(t, extra2, gotExtra2[:])
	gotExtra3 := urkle.LeafExtra(leafTable, 0, 2)
	require.Equal(t, extra3, gotExtra3[:])
}
