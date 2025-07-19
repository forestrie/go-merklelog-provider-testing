package providers

import (
	"fmt"
	"time"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// StorageMassifCommitterFirstMassifTest covers creation of the first massive blob and related conditions
func StorageMassifCommitterFirstMassifTest(tc TestContext) {
	var err error
	// tc := azmmrtesting.NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("Test_mmrMassifCommitter_firstMassif"))

	cfg := tc.GetTestCfg()
	t := tc.GetT()
	ctx := t.Context()

	logID := cfg.LogID

	clock := time.Now()
	// caller should do this, they have the native interface
	// tc.DeleteBlobsByPrefix(tc.StoragePrefix(logID))
	// fmt.Printf("delete: %d\n", time.Since(clock)/time.Millisecond)

	MassifHeight := uint8(3)
	c, err := tc.NewMassifCommitter(storage.Options{LogID: logID, MassifHeight: MassifHeight})
	require.NoError(t, err, "unexpected error creating massif committer")

	var mc *massifs.MassifContext
	clock = time.Now()
	if mc, err = c.GetAppendContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	fmt.Printf("Getxx: %d\n", time.Since(clock)/time.Millisecond)
	assert.Equal(t, mc.Creating, true, "unexpectedly got data, probably tests re-using a container")
	assert.Equal(t, mc.Start.MassifIndex, uint32(0))
}
