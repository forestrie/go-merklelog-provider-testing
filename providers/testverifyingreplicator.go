package providers

import (
	"context"
	"testing"

	"github.com/forestrie/go-merklelog-provider-testing/mmrtesting"
	"github.com/forestrie/go-merklelog/massifs"
	"github.com/forestrie/go-merklelog/massifs/storage"
	"github.com/forestrie/go-merklelog/mmr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// TestReplicateMassifUpdate ensures that an extension to a previously replicated
// massif is handled correctly

func StorageVerifyingReplicatorSinkExtension(
	s *suite.Suite,
	tc mmrtesting.ProviderTestContext,
	sourceFactory BuilderFactory,
	sinkFactory BuilderFactory,
) {
	t := tc.GetT()
	ctx := t.Context()

	h8MassifLeaves := mmr.HeightIndexLeafCount(uint64(8 - 1)) // = ((2 << massifHeight) - 1 + 1) >> 1

	tests := []struct {
		name                   string
		massifHeight           uint8
		firstUpdateMassifs     uint64
		firstUpdateExtraLeaves uint64
		// if zero, the last massif will be completed. If the last is full and secondUpdateMassifs is zero the test is invalid
		secondUpdateMassifs     uint64
		secondUpdateExtraLeaves uint64
	}{
		// extend second massif
		{name: "complete first massif", massifHeight: 8, firstUpdateMassifs: 1, firstUpdateExtraLeaves: h8MassifLeaves - 3, secondUpdateMassifs: 0, secondUpdateExtraLeaves: 3},

		// make sure we cover the obvious edge cases
		{name: "complete first massif", massifHeight: 8, firstUpdateMassifs: 0, firstUpdateExtraLeaves: h8MassifLeaves - 3, secondUpdateMassifs: 0, secondUpdateExtraLeaves: 3},

		// make sure we cover update from partial blob to new massif
		{name: "partial first massif", massifHeight: 8, firstUpdateMassifs: 0, firstUpdateExtraLeaves: h8MassifLeaves - 6, secondUpdateMassifs: 2, secondUpdateExtraLeaves: 0},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			// Populate the log with the content for the first update

			leavesPerMassif := mmr.HeightIndexLeafCount(uint64(tt.massifHeight) - 1) // = ((2 << massifHeight) - 1 + 1) >> 1
			require.True(t, tt.firstUpdateMassifs > 0 || tt.firstUpdateExtraLeaves > 0, uint32(0), "invalid test")
			require.True(t, tt.secondUpdateMassifs > 0 || tt.secondUpdateExtraLeaves > 0, uint32(0), "invalid test")

			// tc.GenerateTenantLog(10, massifHeight, 0 /* leaf type plain */)
			logId0 := tc.GetG().NewLogID()

			sourceBuilder := sourceFactory(tt.massifHeight)
			sinkBuilder := sinkFactory(tt.massifHeight)
			sourceBuilder.SelectLog(ctx, logId0)
			sinkBuilder.SelectLog(ctx, logId0)

			// If we skip CreateLog below, we need to delete the blobs
			sourceBuilder.DeleteLog(logId0)
			sinkBuilder.DeleteLog(logId0)

			if tt.firstUpdateMassifs > 0 {
				tc.CreateLog(
					ctx, sourceBuilder, logId0, tt.massifHeight, uint32(tt.firstUpdateMassifs),
				)
			}
			if tt.firstUpdateExtraLeaves > 0 {
				tc.AddLeaves(
					ctx, sourceBuilder, logId0, tt.massifHeight, tt.firstUpdateMassifs*leavesPerMassif, tt.firstUpdateExtraLeaves,
				)
			}

			type headIndex interface {
				HeadIndex(ctx context.Context, otype storage.ObjectType) (uint32, error)
			}
			assertHeadEq := func(store headIndex, otype storage.ObjectType, expectHead uint32, expectErr error) {
				head, err := store.HeadIndex(ctx, otype)
				if expectErr != nil {
					assert.ErrorIs(t, err, expectErr)
					return
				}
				assert.NoError(t, err)
				assert.Equal(t, expectHead, head)
			}

			assertHeadEq(sourceBuilder.ObjectReader, storage.ObjectMassifData, uint32(tt.firstUpdateMassifs), nil)
			assertHeadEq(sinkBuilder.ObjectReader, storage.ObjectMassifData, uint32(tt.firstUpdateMassifs), storage.ErrDoesNotExist)
			assertHeadEq(sourceBuilder.ObjectReader, storage.ObjectCheckpoint, uint32(tt.firstUpdateMassifs), nil)
			assertHeadEq(sinkBuilder.ObjectReader, storage.ObjectCheckpoint, uint32(tt.firstUpdateMassifs), storage.ErrDoesNotExist)

			// Replicate the log
			vr := massifs.VerifyingReplicator{
				CBORCodec:    tc.GetTestCfg().CBORCodec,
				COSEVerifier: tc.GetTestCfg().COSEVerifier,
				Source:       sourceFactory(tt.massifHeight).ObjectReader,
				Sink:         sinkFactory(tt.massifHeight).ObjectReaderWriter,
			}
			// note: firstUpdateMassifs is the count of *full* massifs we add before adding the leaves, so the count is also the "index" of the last massif.
			err := vr.ReplicateVerifiedUpdates(ctx, uint32(0), uint32(tt.firstUpdateMassifs))
			require.NoError(t, err)
			assertHeadEq(sinkBuilder.ObjectReader, storage.ObjectMassifData, uint32(tt.firstUpdateMassifs), nil)
			assertHeadEq(sinkBuilder.ObjectReader, storage.ObjectCheckpoint, uint32(tt.firstUpdateMassifs), nil)

			// Add the content for the second update

			if tt.secondUpdateMassifs > 0 {
				// CreateLog always deleted blobs, so we can only use AddLeavesToLog here
				for range tt.secondUpdateMassifs {
					tc.AddLeaves(
						ctx, sourceFactory(tt.massifHeight),
						logId0, tt.massifHeight,
						(tt.firstUpdateMassifs*leavesPerMassif)+tt.firstUpdateExtraLeaves,
						leavesPerMassif,
					)
				}
			}

			if tt.secondUpdateExtraLeaves > 0 {
				tc.AddLeaves(
					ctx, sourceFactory(tt.massifHeight),
					logId0, tt.massifHeight,
					(tt.firstUpdateMassifs*leavesPerMassif)+(tt.secondUpdateMassifs*leavesPerMassif)+tt.firstUpdateExtraLeaves,
					tt.secondUpdateExtraLeaves,
				)
			}

			vr = massifs.VerifyingReplicator{
				CBORCodec:    tc.GetTestCfg().CBORCodec,
				COSEVerifier: tc.GetTestCfg().COSEVerifier,
				Source:       sourceFactory(tt.massifHeight).ObjectReader,
				Sink:         sinkFactory(tt.massifHeight).ObjectReaderWriter,
			}
			// note: firstUpdateMassifs is the count of *full* massifs we add before adding the leaves, so the count is also the "index" of the last massif.
			err = vr.ReplicateVerifiedUpdates(ctx, uint32(0), uint32(tt.firstUpdateMassifs+tt.secondUpdateMassifs))
			require.NoError(t, err)

			assertHeadEq(vr.Sink, storage.ObjectMassifData, uint32(tt.firstUpdateMassifs+tt.secondUpdateMassifs), nil)
			assertHeadEq(vr.Sink, storage.ObjectCheckpoint, uint32(tt.firstUpdateMassifs+tt.secondUpdateMassifs), nil)

			// Attempt to replicate again, this will verify the sink state and then do nothing
			vr = massifs.VerifyingReplicator{
				Source: sourceFactory(tt.massifHeight).ObjectReader,
				Sink:   sinkFactory(tt.massifHeight).ObjectReaderWriter,
			}
			// note: firstUpdateMassifs is the count of *full* massifs we add before adding the leaves, so the count is also the "index" of the last massif.
			err = vr.ReplicateVerifiedUpdates(ctx, uint32(0), uint32(tt.firstUpdateMassifs+tt.secondUpdateMassifs))
			require.NoError(t, err)

			assertHeadEq(vr.Sink, storage.ObjectMassifData, uint32(tt.firstUpdateMassifs+tt.secondUpdateMassifs), nil)
			assertHeadEq(vr.Sink, storage.ObjectCheckpoint, uint32(tt.firstUpdateMassifs+tt.secondUpdateMassifs), nil)

			s.NoError(err)
		})
	}
}

// StorageVerifyingReplicatorSinkTamperDetected tests that a tampered sink is detected
//
// In this case, an attacker changes a remotely replicated massif in an attempt to
// include, exclude or change some element. In order for such a change to be
// provable, the attacker has to re-build the log from the point of the tamper
// forward, otherwise the inclusion proof for the changed element will fail.  We
// can simulate this situation without re-building the log simply by changing
// one of the peaks, as a re-build will necessarily always result in a different
// peak value.
//
// Attacks where the leaves are changed or remove and the log is not re-built
// can only be detected by full audit anyway. But these attacks are essentially
// equivalent to data corruption. And they do not result in a log which includes
// a different thing, just a single entry (or pair of) in the log which can't be
// proven
func StorageVerifyingReplicatorSinkTamperDetected(
	tc mmrtesting.ProviderTestContext,
	sourceFactory BuilderFactory,
	sinkFactory BuilderFactory,
) {

	var err error
	t := tc.GetT()
	ctx := t.Context()

	massifCount := uint32(4)
	massifHeight := uint8(8)

	leavesPerMassif := mmr.HeightIndexLeafCount(uint64(massifHeight) - 1)

	// This test requires two invocations. For the first invocation, we make ony
	// one massif available.  Then after that is successfully replicated, we
	// tamper a peak in the local replica, then attempt to replicate the
	// subsequent log - this should fail due to the local data being unable to
	// re-produce the root needed for the local seal to verify.
	logId0 := tc.GetG().NewLogID()

	sourceBuilder := sourceFactory(massifHeight)
	sinkBuilder := sinkFactory(massifHeight)
	sourceBuilder.SelectLog(ctx, logId0)
	sinkBuilder.SelectLog(ctx, logId0)

	// note: CreateLog both creates the massifs *and* populates them
	// provider test context implementations always default to creating for the *source*
	tc.CreateLog(ctx, sourceBuilder, logId0, massifHeight, 1)

	vr := massifs.VerifyingReplicator{
		CBORCodec:    tc.GetTestCfg().CBORCodec,
		COSEVerifier: tc.GetTestCfg().COSEVerifier,
		Source:       sourceBuilder.ObjectReader,
		Sink:         sinkBuilder.ObjectReaderWriter,
	}
	err = vr.ReplicateVerifiedUpdates(ctx, uint32(0), uint32(1))
	require.NoError(t, err)

	massifLeafCount := mmr.HeightIndexLeafCount(uint64(massifHeight) - 1)
	LastLeafIndex := massifLeafCount - 1
	mmrSize0 := mmr.FirstMMRSize(mmr.MMRIndex(LastLeafIndex))
	peaks := mmr.Peaks(mmrSize0 - 1)

	// this simulates the effect of changing a leaf then re-building the log so
	// that a proof of inclusion can be produced for the new element, this
	// necessarily causes a peak to change. *any* peak change will cause the
	// consistency proof to fail. And regardless of whether our seals are
	// accumulators (all peak hashes) or a single bagged peak, the local log
	// will be unable to produce the correct detached payload for the Sign1 seal
	// over the root material.
	tamperSinkNode(t, sourceBuilder.ObjectReaderWriter,
		massifHeight, peaks[len(peaks)-1], []byte{0x0D, 0x0E, 0x0A, 0x0D, 0x0B, 0x0E, 0x0E, 0x0F})

	// Note: it's actually a property of the way massifs fill that the last node
	// added is always a peak, we could have taken that shortcut above. In the
	// interests of illustrating how any peak can be found, its done the long
	// way above.

	// add the rest of the massifs
	for i := uint32(1); i < massifCount; i++ {
		tc.AddLeaves(ctx, sourceBuilder, logId0, massifHeight, uint64(uint64(i)*leavesPerMassif), uint64(leavesPerMassif))
	}

	sourceBuilder = sourceFactory(massifHeight)
	sinkBuilder = sinkFactory(massifHeight)
	sourceBuilder.SelectLog(ctx, logId0)
	sinkBuilder.SelectLog(ctx, logId0)

	// Note: we create a new context so that implementations can be fooled by cached state
	vr = massifs.VerifyingReplicator{
		CBORCodec:    tc.GetTestCfg().CBORCodec,
		COSEVerifier: tc.GetTestCfg().COSEVerifier,
		Sink:         sinkBuilder.ObjectReaderWriter,
		Source:       sourceBuilder.ObjectReader,
	}
	err = vr.ReplicateVerifiedUpdates(ctx, uint32(0), massifCount-1)
	require.ErrorIs(t, err, massifs.ErrSealVerifyFailed)

	// check the 0'th massifs and seals was replicated (by the first run)
	_, err = massifs.GetMassifContext(ctx, vr.Sink, 0)
	require.NoError(t, err)
	_, err = massifs.GetCheckpoint(ctx, vr.Sink, vr.CBORCodec, 0)
	require.NoError(t, err)

	// check the massifs from the second veracity run were NOT replicated
	for i := uint32(1); i < massifCount; i++ {

		_, err = massifs.GetMassifContext(ctx, vr.Sink, i)
		require.ErrorIs(t, err, storage.ErrDoesNotExist)
		_, err = massifs.GetCheckpoint(ctx, vr.Sink, vr.CBORCodec, i)
		require.NoError(t, err, storage.ErrDoesNotExist)
	}
}

// tamperSinkNode over-writes the log entry at the given mmrIndex with the provided bytes
// This is typically used to simulate a local tamper or corruption
//
// The value needs to be non-empty and no longer than LogEntryBytes, a fine
// value for this purpose is:
//
//	[]byte{0x0D, 0x0E, 0x0A, 0x0D, 0x0B, 0x0E, 0x0E, 0x0F}
func tamperSinkNode(
	t *testing.T, objectStore massifs.ObjectReaderWriter,
	massifHeight uint8, mmrIndex uint64, tamperedValue []byte,
) {
	var err error
	require.NotZero(t, len(tamperedValue))
	require.LessOrEqual(t, len(tamperedValue), massifs.LogEntryBytes)

	leafIndex := mmr.LeafIndex(mmrIndex)
	massifIndex := uint32(massifs.MassifIndexFromLeafIndex(massifHeight, leafIndex))

	// err = objectStore.SelectLog(context.TODO(), logID)
	// require.NoError(t, err)

	mc, err := massifs.GetMassifContext(t.Context(), objectStore, massifIndex)
	require.NoError(t, err)

	i := mmrIndex - mc.Start.FirstIndex
	logData := mc.Data[mc.LogStart():]
	copy(logData[i*massifs.LogEntryBytes:i*massifs.LogEntryBytes+8], tamperedValue)
	objectStore.Put(t.Context(), massifIndex, storage.ObjectMassifData, mc.Data, false)
}
