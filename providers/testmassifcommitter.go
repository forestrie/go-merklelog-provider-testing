package providers

import (
	"fmt"
	"time"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/robinbryce/go-merklelog-provider-testing/mmrtesting"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// StorageMassifCommitterFirstMassifTest covers creation of the first massif with generic storage
func StorageMassifCommitterFirstMassifTest(tc mmrtesting.ProviderTestContext) {
	var err error
	cfg := tc.GetTestCfg()
	t := tc.GetT()
	ctx := t.Context()

	logID := cfg.LogID

	MassifHeight := uint8(3)
	c, err := tc.NewMassifCommitter(massifs.StorageOptions{LogID: logID, MassifHeight: MassifHeight})
	require.NoError(t, err, "unexpected error creating massif committer")
	var mc *massifs.MassifContext
	clock := time.Now()
	if mc, err = c.GetAppendContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	fmt.Printf("GetAppendContext: %d\n", time.Since(clock)/time.Millisecond)
	assert.Equal(t, mc.Creating, true, "unexpectedly got data, probably tests re-using a container")
	assert.Equal(t, mc.Start.MassifIndex, uint32(0))
}

// StorageMassifCommitterAddFirstTwoLeavesTest tests adding first two leaves with generic storage
func StorageMassifCommitterAddFirstTwoLeavesTest(tc mmrtesting.ProviderTestContext) {
	var err error
	cfg := tc.GetTestCfg()
	t := tc.GetT()
	ctx := t.Context()

	logID := cfg.LogID
	MassifHeight := uint8(3)
	c, err := tc.NewMassifCommitter(massifs.StorageOptions{LogID: logID, MassifHeight: MassifHeight})
	require.NoError(t, err, "unexpected error creating massif committer")

	var mc *massifs.MassifContext
	if mc, err = c.GetAppendContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// manually insert the appropriate log entries, to separate this test from
	// those that cover the mmr construction and how the massifs link together
	mc.Data = tc.PadWithNumberedLeaves(mc.Data, 0, 2)

	err = c.CommitContext(ctx, mc)
	assert.Nil(t, err)

	// Ensure what we read back passes the commit checks
	if _, err = c.GetAppendContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

// StorageMassifCommitterExtendAndCommitFirstTest tests massif extension with generic storage
func StorageMassifCommitterExtendAndCommitFirstTest(tc mmrtesting.ProviderTestContext) {
	var err error
	cfg := tc.GetTestCfg()
	t := tc.GetT()
	ctx := t.Context()

	logID := cfg.LogID
	MassifHeight := uint8(3)
	c, err := tc.NewMassifCommitter(massifs.StorageOptions{LogID: logID, MassifHeight: MassifHeight})
	require.NoError(t, err, "unexpected error creating massif committer")

	var mc *massifs.MassifContext
	if mc, err = c.GetAppendContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// add 3 entries, leaving space for two more logs
	mc.Data = tc.PadWithNumberedLeaves(mc.Data, 0, 3)
	err = c.CommitContext(t.Context(), mc)
	assert.Nil(t, err)

	if mc, err = c.GetAppendContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assert.Equal(t, mc.Creating, false)
}

// StorageMassifCommitterCompleteFirstTest tests massif completion with generic storage
func StorageMassifCommitterCompleteFirstTest(tc mmrtesting.ProviderTestContext) {
	var err error
	cfg := tc.GetTestCfg()
	t := tc.GetT()
	ctx := t.Context()

	logID := cfg.LogID
	MassifHeight := uint8(3)
	c, err := tc.NewMassifCommitter(massifs.StorageOptions{LogID: logID, MassifHeight: MassifHeight})
	require.NoError(t, err, "unexpected error creating massif committer")

	var mc *massifs.MassifContext
	if mc, err = c.GetAppendContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	first := 0
	// add first two entries, representing the first actual leaf and the interior root node it creates
	mc.Data = tc.PadWithNumberedLeaves(mc.Data, first, 2)
	first += 2
	err = c.CommitContext(t.Context(), mc)
	assert.Nil(t, err)

	// Ensure what we read back passes the commit checks
	if mc, err = c.GetAppendContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assert.Equal(t, mc.Creating, false)

	// add 5 entries, completing the first massif
	mc.Data = tc.PadWithNumberedLeaves(mc.Data, first, 5)
	first += 5
	err = c.CommitContext(t.Context(), mc)
	assert.Nil(t, err)

	// Ensure that when we ask for a new context, we get an empty one that is in create mode.
	if mc, err = c.GetAppendContext(t.Context()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assert.Equal(t, mc.Creating, true)
}

// StorageMassifCommitterOverfillSafeTest tests overfill protection with generic storage
func StorageMassifCommitterOverfillSafeTest(tc mmrtesting.ProviderTestContext) {
	var err error
	cfg := tc.GetTestCfg()
	t := tc.GetT()
	ctx := t.Context()

	logID := cfg.LogID
	MassifHeight := uint8(3)
	c, err := tc.NewMassifCommitter(massifs.StorageOptions{LogID: logID, MassifHeight: MassifHeight})
	require.NoError(t, err, "unexpected error creating massif committer")

	var mc *massifs.MassifContext
	if mc, err = c.GetAppendContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	first := 0
	// add first two entries, representing the first actual leaf and the interior root node it creates
	mc.Data = tc.PadWithNumberedLeaves(mc.Data, first, 2)
	first += 2
	err = c.CommitContext(t.Context(), mc)
	assert.Nil(t, err)

	// Test context reuse - this should work just like the original implementation
	// The unified implementation should properly update metadata state after each commit

	// add 3 entries, leaving space for two more logs (reusing same context)
	mc.Data = tc.PadWithNumberedLeaves(mc.Data, first, 3)
	err = c.CommitContext(t.Context(), mc)
	assert.Nil(t, err)

	// add 5 entries, over filling the first massif (still reusing same context)
	mc.Data = tc.PadWithNumberedLeaves(mc.Data, first, 5)
	err = c.CommitContext(t.Context(), mc)
	if err == nil {
		t.Fatalf("overfilled massif")
	}
}

// StorageMassifCommitterThreeMassifsTest tests three massifs scenario with generic storage
func StorageMassifCommitterThreeMassifsTest(tc mmrtesting.ProviderTestContext) {
	var err error
	cfg := tc.GetTestCfg()
	t := tc.GetT()
	ctx := t.Context()

	logID := cfg.LogID

	// Height of 3 means each massif will contain 7 nodes.
	MassifHeight := uint8(3)

	c, err := tc.NewMassifCommitter(massifs.StorageOptions{LogID: logID, MassifHeight: MassifHeight})
	require.NoError(t, err, "unexpected error creating massif committer")

	// --- Massif 0

	var mc *massifs.MassifContext
	if mc, err = c.GetAppendContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	first := 0
	// add all the entries for the first massif
	mc.Data = tc.PadWithNumberedLeaves(mc.Data, first, 7)
	first += 7
	require.Equal(t, uint64(7), mc.RangeCount())

	err = c.CommitContext(t.Context(), mc)
	assert.Nil(t, err)

	// --- Massif 1

	// get the next context, it should be a 'creating' context. This is an edge
	// case as massif 0 is always exactly filled - the mmr root and the massif
	// root are the same only for this blob
	if mc, err = c.GetAppendContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assert.Equal(t, mc.Creating, true)
	assert.Equal(t, len(mc.Data)-int(mc.LogStart()), 0)
	// Check our start leaf value is the last hash from the previous mmr
	assert.Equal(t, mc.Start.FirstIndex, uint64(7))

	// to fill massif 1, we need to add a single alpine node (one which depends on a prior massif)
	require.Equal(t, mc.RangeCount(), uint64(7))
	mc.Data = tc.PadWithNumberedLeaves(mc.Data, first, 8)
	first += 8
	require.Equal(t, uint64(15), mc.RangeCount())

	// commit it
	err = c.CommitContext(ctx, mc)
	assert.Nil(t, err)

	// --- Massif 2

	// get the context for the third, this should also be creating
	if mc, err = c.GetAppendContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assert.Equal(t, mc.Creating, true)
	assert.Equal(t, len(mc.Data)-int(mc.LogStart()), 0)
	assert.Equal(t, mc.Start.FirstIndex, uint64(15))

	// fill it, note that this one does _not_ require an alpine node
	mc.Data = tc.PadWithNumberedLeaves(mc.Data, first, 7)
	first += 7
	require.Equal(t, uint64(22), mc.RangeCount())

	err = c.CommitContext(ctx, mc)
	assert.Nil(t, err)

	// --- Massif 3
	if mc, err = c.GetAppendContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assert.Equal(t, mc.Start.FirstIndex, uint64(22))
	assert.Equal(t, mc.Creating, true)

	// *part* fill it
	mc.Data = tc.PadWithNumberedLeaves(mc.Data, first, 2)
	err = c.CommitContext(ctx, mc)
	assert.Nil(t, err)

	if mc, err = c.GetAppendContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assert.Equal(t, mc.Creating, false)
	assert.Equal(t, mc.Start.FirstIndex, uint64(22))
}
