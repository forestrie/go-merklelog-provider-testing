package mmrtesting

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/datatrails/go-datatrails-merklelog/mmr"
	"github.com/forestrie/go-merklelog-datatrails/datatrails"
	"github.com/stretchr/testify/require"

	// TODO: phase this out

	"github.com/datatrails/go-datatrails-common/logger"
)

const (
	DefaultCheckpointIssuer = "https://github.com/robinbryce/testing/checkpoint-issuer/default"
)

// ProviderTestContext is satisfied by storage specific provider implementations
// Doing so allows them to be tested using the common provider tests.
type ProviderTestContext interface {
	GetTestCfg() TestOptions
	GetT() *testing.T
	GetG() *TestGenerator

	// Generation methods
	PadWithNumberedLeaves(data []byte, first, n int) []byte

	CommitLeaves(
		ctx context.Context,
		builder LogBuilder,
		base uint64,
		count uint64,
	) (GeneratedLeaves, error)

	CreateLog(
		ctx context.Context,
		builder LogBuilder,
		massifHeight uint8, massifCount uint32,
	) (GeneratedLeaves, error)

	AddLeaves(
		ctx context.Context,
		builder LogBuilder,
		base uint64,
		count uint64,
	) (GeneratedLeaves, error)
}

type MassifPutter interface {
	Put(ctx context.Context, massifIndex uint32, ty storage.ObjectType, data []byte, failIfExists bool) error
}

type HeadSealer interface {
	Put(ctx context.Context, massifIndex uint32, ty storage.ObjectType, data []byte, failIfExists bool) error
	HeadIndex(ctx context.Context, otype storage.ObjectType) (uint32, error)
	GetCheckpoint(ctx context.Context, massifIndex uint32) (*massifs.Checkpoint, error)
	GetMassifContext(ctx context.Context, massifIndex uint32) (*massifs.MassifContext, error)
}

// MassifSealer provides the minimal storage interface required to implement log sealing for tests
type MassifSealer interface {
	GetStorageOptions() massifs.StorageOptions
	HeadIndex(ctx context.Context, otype storage.ObjectType) (uint32, error)
	GetMassifContext(ctx context.Context, massifIndex uint32) (*massifs.MassifContext, error)
	GetCheckpoint(ctx context.Context, massifIndex uint32) (*massifs.Checkpoint, error)
	Put(ctx context.Context, massifIndex uint32, ty storage.ObjectType, data []byte, failIfExists bool) error
}

type MassifCommitter interface {
	GetAppendContext(ctx context.Context) (*massifs.MassifContext, error)
	CommitContext(ctx context.Context, mc *massifs.MassifContext) error
}

type MassifGetter interface {
	GetMassifContext(ctx context.Context, massifIndex uint32) (*massifs.MassifContext, error)
}

// MassifStore provides the storage interface required to implement log creation and appending for tests
type MassifStore interface {
	// GetStorageOptions() massifs.StorageOptions
	SelectLog(ctx context.Context, logId storage.LogID) error
	HeadIndex(ctx context.Context, otype storage.ObjectType) (uint32, error)
	GetAppendContext(ctx context.Context) (*massifs.MassifContext, error)
	CommitContext(ctx context.Context, mc *massifs.MassifContext) error
	GetMassifContext(ctx context.Context, massifIndex uint32) (*massifs.MassifContext, error)
	GetCheckpoint(ctx context.Context, massifIndex uint32) (*massifs.Checkpoint, error)
	Put(ctx context.Context, massifIndex uint32, ty storage.ObjectType, data []byte, failIfExists bool) error
}

type ObjectStore interface {
	// GetStorageOptions() massifs.StorageOptions
	SelectLog(ctx context.Context, logId storage.LogID) error
	HeadIndex(ctx context.Context, otype storage.ObjectType) (uint32, error)
	GetMassifContext(ctx context.Context, massifIndex uint32) (*massifs.MassifContext, error)
	GetCheckpoint(ctx context.Context, massifIndex uint32) (*massifs.Checkpoint, error)
	GetStart(ctx context.Context, massifIndex uint32) (*massifs.MassifStart, error)
	Put(ctx context.Context, massifIndex uint32, ty storage.ObjectType, data []byte, failIfExists bool) error
}

type MassifStorageEmulator interface {
	// Init(t *testing.T, cfg *TestOptions)
	DeleteLog(logID storage.LogID)
	DeleteByStoragePrefix(storagePrefixPath string)
}

type LogDeleter func(logID storage.LogID)

type LogBuilder struct {

	// LeafGenerator creates leaf content and the associated AddLeafArgs for adding it to a massif log
	LeafGenerator LeafGenerator
	// MassifCommitter is the regular minimal interface for extending a massif log
	MassifCommitter MassifCommitter

	// If this is not nil and DisableSealing is false then the log will be sealed after leaves are added
	MassifSealer MassifSealer

	// Used for general inspection of the store, all providers have to be able to satisfy this
	ObjectStore ObjectStore

	DeleteLog LogDeleter
}

type TestContext[E MassifStorageEmulator] struct {
	Cfg      *TestOptions
	T        *testing.T
	Emulator E
	G        TestGenerator
}

func NewTestContext[E MassifStorageEmulator](
	t *testing.T, emulator E, opts *TestOptions) *TestContext[E] {

	c := TestContext[E]{
		T:        t,
		Emulator: emulator,
	}
	c.Init(t, opts)

	return &c
}

func (c *TestContext[E]) GetT() *testing.T {
	return c.T
}

func (c *TestContext[E]) GetG() *TestGenerator {
	return &c.G
}

func (c *TestContext[E]) DeleteLog(logID storage.LogID) {
	if logID == nil {
		return
	}

	c.Emulator.DeleteByStoragePrefix(datatrails.StoragePrefixPath(logID))
}

func (c *TestContext[E]) Init(t *testing.T, opts *TestOptions) {

	opts.EnsureDefaults(t)

	// require.NotNil(t, opts.FSDir, "we must have a FSDir if we are using filesystem storage")

	logger.New(opts.LogLevel)
	c.T = t

	c.G.Init(t, opts)
	c.Cfg = opts

	c.Emulator.DeleteByStoragePrefix(datatrails.StoragePrefixPath(c.Cfg.LogID))

	require.True(t, opts.DisableSigning || opts.Signer != nil, "unless signing is disabled we must have a putter")

	c.Cfg = opts
}

func (tc *TestContext[E]) PadWithNumberedLeaves(data []byte, first, n int) []byte {
	return PadWithNumberedLeaves(data, first, n)
}

func (tc *TestContext[E]) CommitLeaves(
	ctx context.Context,
	builder LogBuilder,
	base uint64,
	count uint64,
) (GeneratedLeaves, error) {
	if count <= 0 {
		return GeneratedLeaves{}, nil
	}
	t := tc.T
	mc, err := builder.MassifCommitter.GetAppendContext(ctx)
	require.NoError(t, err)

	generated := GeneratedLeaves{}

	for i := uint64(0); i < count; i++ {

		addArgs, content := builder.LeafGenerator.Generate(base, i)

		generated.Encoded = append(generated.Encoded, content)
		generated.Args = append(generated.Args, addArgs)
		generated.LeafIndices = append(generated.LeafIndices, base+i)

		// TODO: decide if we need to support post-encoding decoration per datatrails
		// mmrIndex is equal to the count of all nodes
		generated.MMRIndices = append(generated.MMRIndices, mc.RangeCount())

		_, err = mc.AddHashedLeaf(
			sha256.New(), addArgs.ID, nil, addArgs.LogID, addArgs.AppID, addArgs.Value)
		if errors.Is(err, massifs.ErrMassifFull) {
			err = builder.MassifCommitter.CommitContext(ctx, mc)
			if err != nil {
				return generated, err
			}
			mc, err = builder.MassifCommitter.GetAppendContext(ctx)
			if err != nil {
				return generated, err
			}

			// Remember to add the leaf we failed to add above
			_, err = mc.AddHashedLeaf(
				sha256.New(), addArgs.ID, nil, addArgs.LogID, addArgs.AppID, addArgs.Value)
			if err != nil {
				return generated, err
			}

			err = nil
		}
		if err != nil {
			return generated, err
		}
	}
	err = builder.MassifCommitter.CommitContext(ctx, mc)
	if err != nil {
		return generated, err
	}

	return generated, nil
}

func (tc *TestContext[E]) CreateLog(
	ctx context.Context,
	builder LogBuilder,
	// massifHeight must correspond to the height used to create the committer
	// in order for massifCount to be accurately interpreted
	massifHeight uint8, massifCount uint32,
) (GeneratedLeaves, error) {

	tc.DeleteLog(builder.LeafGenerator.LogID)

	leavesPerMassif := mmr.HeightIndexLeafCount(uint64(massifHeight) - 1)
	count := leavesPerMassif * uint64(massifCount)

	generated, err := tc.AddLeaves(ctx, builder, 0, count)
	require.NoError(tc.T, err)
	return generated, nil
}

func (tc *TestContext[E]) AddLeaves(
	ctx context.Context,
	builder LogBuilder,
	base uint64,
	count uint64,
) (GeneratedLeaves, error) {
	if count <= 0 {
		return GeneratedLeaves{}, nil
	}

	headIndexBefore, err := builder.MassifSealer.HeadIndex(ctx, storage.ObjectMassifData)
	if errors.Is(err, storage.ErrDoesNotExist) || errors.Is(err, storage.ErrLogEmpty) {
		headIndexBefore = 0
		err = nil
	}
	require.NoError(tc.T, err)

	generated, err := tc.CommitLeaves(ctx, builder, base, count)
	if err != nil {
		return generated, err
	}

	if !tc.Cfg.DisableSigning && builder.MassifSealer != nil {
		headIndex, err := builder.MassifSealer.HeadIndex(ctx, storage.ObjectMassifData)
		require.NoError(tc.T, err)
		for i := headIndexBefore; i <= headIndex; i++ {
			_, err := tc.SealIndex(ctx, builder.MassifSealer, i)
			require.NoError(tc.T, err)
		}
	}

	return generated, nil
}

func (tc *TestContext[E]) SealIndex(
	ctx context.Context, store HeadSealer, massifIndex uint32) (*massifs.Checkpoint, error) {

	mc, err := store.GetMassifContext(ctx, massifIndex)
	require.NoError(tc.T, err)
	err = mc.CreatePeakStackMap()
	require.NoError(tc.T, err)

	chk, err := store.GetCheckpoint(ctx, massifIndex)
	if err != nil {
		if errors.Is(err, storage.ErrDoesNotExist) || errors.Is(err, storage.ErrLogEmpty) {
			return tc.SealContext(ctx, store, mc, chk)
		}
		return nil, err
	}

	if chk.MMRState.MMRSize < mc.RangeCount() {
		return tc.SealContext(ctx, store, mc, chk)
	}
	// already sealed (the equals case), or the seal is ahead of the massif.
	// this is allowed here because we are supporting test code which may be
	// purposfully setting up adverse conditions.
	return chk, nil
}

func (tc *TestContext[E]) SealHead(
	ctx context.Context, store HeadSealer) (*massifs.Checkpoint, error) {

	massifIndex, err := store.HeadIndex(ctx, storage.ObjectMassifData)
	require.NoError(tc.T, err)
	mc, err := store.GetMassifContext(ctx, massifIndex)
	require.NoError(tc.T, err)
	err = mc.CreatePeakStackMap()
	require.NoError(tc.T, err)

	var chk *massifs.Checkpoint

	// Note: we seal the head massif, if there are higher seals we ignore them.
	chkIndex, err := store.HeadIndex(ctx, storage.ObjectCheckpoint)
	if err != nil {
		if errors.Is(err, storage.ErrDoesNotExist) || errors.Is(err, storage.ErrLogEmpty) {
			chk = nil
			err = nil
		}
	} else {
		// we  require that the caller is building and signing a log incrementally and so the head seal is always
		// the current massif or the one before it.
		require.GreaterOrEqual(
			tc.T, chkIndex+1, massifIndex,
			"you must build and seal the log incrementally. the seal cannot be more than one massif behind")

		chk, err = store.GetCheckpoint(ctx, chkIndex)
		if err != nil {
			return nil, fmt.Errorf("failed to get checkpoint: %w", err)
		}
	}

	require.NoError(tc.T, err)
	chk, err = tc.SealContext(ctx, store, mc, chk)
	require.NoError(tc.T, err)
	return chk, nil
}

func (tc *TestContext[E]) SealContext(
	ctx context.Context, store MassifPutter, mc *massifs.MassifContext, chk *massifs.Checkpoint) (*massifs.Checkpoint, error) {

	var err error
	var peaksB [][]byte

	mmrSizeNew := mc.RangeCount()

	if false && chk != nil {
		var ok bool
		mmrFromIndex := uint64(0)
		mmrFromIndex = chk.MMRState.MMRSize - 1
		cp, err := mmr.IndexConsistencyProof(mc, mmrFromIndex, mmrSizeNew-1)
		if err != nil {
			require.NoError(tc.T, err)
		}

		// a := massifs.TreeCount(mc.Start.MassifHeight)
		// b := massifs.MassifIndexFromMMRIndex(mc.Start.MassifHeight, mmrFromIndex)
		// c := massifs.MassifIndexFromMMRIndex(mc.Start.MassifHeight, mmrSizeNew-1)
		// fmt.Printf("a %d, b %d, c %d\n", a, b, c)

		chkPeaks, err := mmr.PeakHashes(mc, chk.MMRState.MMRSize-1)
		require.NoError(tc.T, err)

		ok, peaksB, err = mmr.CheckConsistency(
			mc, sha256.New(),
			cp.MMRSizeA, cp.MMRSizeB, chkPeaks)

		require.NoError(tc.T, err)
		require.True(tc.T, ok, "consistency check failed: verify failed")
	} else {
		// Just sign the peaks as is. Clearly this is unsafe unless the caller knows what they are doing.
		peaksB, err = mmr.PeakHashes(mc, mmrSizeNew-1)
		require.NoError(tc.T, err)
	}
	lastIDTimestamp := mc.GetLastIDTimestamp()

	state := massifs.MMRState{
		Version:         int(massifs.MMRStateVersionCurrent),
		MMRSize:         mmrSizeNew,
		Peaks:           peaksB,
		Timestamp:       time.Now().UnixMilli(),
		CommitmentEpoch: mc.Start.CommitmentEpoch,
		IDTimestamp:     lastIDTimestamp,
	}
	cborCodec, err := massifs.NewRootSignerCodec()
	require.NoError(tc.T, err)

	rootSigner := massifs.NewRootSigner("https://github.com/robinbryce/veracity", cborCodec)

	data, err := rootSigner.Sign1(
		tc.Cfg.Signer,
		DefaultCheckpointIssuer,
		tc.Cfg.PubKey,
		"test-log-checkpoint",
		state, nil,
	)
	require.NoError(tc.T, err)

	// Write checkpoint directly using Azure blob storage
	// storagePath, err := tc.PathProvider.GetStoragePath(mc.Start.MassifIndex, storage.ObjectCheckpoint)
	// require.NoError(tc.T, err)
	err = store.Put(ctx, mc.Start.MassifIndex, storage.ObjectCheckpoint, data, false)
	require.NoError(tc.T, err)

	// make the checkpoint for the next call to base itself off of

	msg, state2, err := massifs.DecodeSignedRoot(cborCodec, data)
	require.NoError(tc.T, err)
	return &massifs.Checkpoint{
		Sign1Message: *msg,
		MMRState:     state2,
	}, nil
}
