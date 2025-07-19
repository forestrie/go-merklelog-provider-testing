package providers

import (
	"fmt"
	"time"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// StorageMassifCommitterFirstMassifTest covers creation of the first massive and related conditions
func StorageMassifCommitterFirstMassifTest(tc TestContext) {
	var err error
	// tc := azmmrtesting.NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("Test_mmrMassifCommitter_firstMassif"))

	cfg := tc.GetTestCfg()
	t := tc.GetT()
	ctx := t.Context()

	logID := cfg.LogID

	MassifHeight := uint8(3)
	c, err := tc.NewMassifCommitter(storage.Options{LogID: logID, MassifHeight: MassifHeight})
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

// StorageMassifCommitterAddFirstTwoLeavesTest the addition of leaves to a new log whose height is sufficient to contain at least two leaves
func StorageMassifCommitterAddFirstTwoLeavesTest(tc TestContext) {
	var err error
	cfg := tc.GetTestCfg()
	t := tc.GetT()
	ctx := t.Context()

	logID := cfg.LogID
	MassifHeight := uint8(3)
	c, err := tc.NewMassifCommitter(storage.Options{LogID: logID, MassifHeight: MassifHeight})
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

// StorageMassifCommitterExtendAndCommitFirstTest the addition, and committal of 3 leaves to a new log whose height is sufficient to contain more than 3 leaves.
func StorageMassifCommitterExtendAndCommitFirstTest(tc TestContext) {
	var err error
	cfg := tc.GetTestCfg()
	t := tc.GetT()
	ctx := t.Context()

	logID := cfg.LogID
	MassifHeight := uint8(3)
	c, err := tc.NewMassifCommitter(storage.Options{LogID: logID, MassifHeight: MassifHeight})
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

// StorageMassifCommitterCompleteFirstTest test that aquiring a context after
// perfectly filling a massif results in a new empty context ready for adding
// further entries
func StorageMassifCommitterCompleteFirstTest(tc TestContext) {
	var err error
	cfg := tc.GetTestCfg()
	t := tc.GetT()
	ctx := t.Context()

	logID := cfg.LogID
	MassifHeight := uint8(3)
	c, err := tc.NewMassifCommitter(storage.Options{LogID: logID, MassifHeight: MassifHeight})
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

func StorageMassifCommitterOverfillSafeTest(tc TestContext) {
	var err error
	cfg := tc.GetTestCfg()
	t := tc.GetT()
	ctx := t.Context()

	logID := cfg.LogID
	MassifHeight := uint8(3)
	c, err := tc.NewMassifCommitter(storage.Options{LogID: logID, MassifHeight: MassifHeight})
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

	// Implementations should be create safe, that is after committing a massif that is in "Creating"
	// mode, subsequent use of the context to add more entries is possible, without having to call GetAppendContext
	// if mc, err = c.GetAppendContext(ctx); err != nil {
	// 	t.Fatalf("unexpected err: %v", err)
	// }

	// add 3 entries, leaving space for two more logs
	mc.Data = tc.PadWithNumberedLeaves(mc.Data, first, 3)
	err = c.CommitContext(t.Context(), mc)
	assert.Nil(t, err)

	// add 5 entries, over filling the first massif
	mc.Data = tc.PadWithNumberedLeaves(mc.Data, first, 5)
	err = c.CommitContext(t.Context(), mc)
	if err == nil {
		t.Fatalf("overfilled massif")
	}
}

func StorageMassifCommitterThreeMassifsTest(tc TestContext) {

	var err error
	cfg := tc.GetTestCfg()
	t := tc.GetT()
	ctx := t.Context()

	logID := cfg.LogID

	// Height of 3 means each massif will contain 7 nodes.
	MassifHeight := uint8(3)

	c, err := tc.NewMassifCommitter(storage.Options{LogID: logID, MassifHeight: MassifHeight})
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
	// blobPath1 := fmt.Sprintf("v1/mmrs/tenant/%s/0/massifs/%016d.log", logIDStr, 1)
	// ac := c.Az.Massifs[mc.Start.MassifIndex]
	// assert.Equal(tc.T, ac.BlobPath, blobPath1)
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
	// blobPath2 := fmt.Sprintf("v1/mmrs/tenant/%s/0/massifs/%016d.log", logIDStr, 2)
	// ac = c.Az.Massifs[mc.Start.MassifIndex]
	// assert.Equal(tc.T, ac.BlobPath, blobPath2)
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
	// blobPath3 := fmt.Sprintf("v1/mmrs/tenant/%s/0/massifs/%016d.log", logIDStr, 3)
	// ac = c.Az.Massifs[mc.Start.MassifIndex]
	// assert.Equal(tc.T, ac.BlobPath, blobPath3)

	// *part* fill it
	mc.Data = tc.PadWithNumberedLeaves(mc.Data, first, 2)
	err = c.CommitContext(ctx, mc)
	assert.Nil(t, err)

	if mc, err = c.GetAppendContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assert.Equal(t, mc.Creating, false)
	// assert.Equal(t, c.Az.Massifs[mc.Start.MassifIndex].BlobPath, blobPath3)
	assert.Equal(t, mc.Start.FirstIndex, uint64(22))
}
