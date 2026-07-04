package mmrtesting

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"testing"

	"github.com/forestrie/go-merklelog/massifs"
	"github.com/forestrie/go-merklelog/massifs/cose"
	"github.com/forestrie/go-merklelog/massifs/storage"
)

// TestSignerContext is a self-contained checkpoint signer for tests: an
// ephemeral P-256 key wrapped in a COSE signer producing format-v3
// checkpoint receipts.
type TestSignerContext struct {
	Key        ecdsa.PrivateKey
	CoseSigner *cose.TestCoseSigner
}

func NewTestSignerContext(t *testing.T, issuer string) *TestSignerContext {
	_ = issuer // v3 receipts carry no issuer claims; retained for call-site compatibility
	key := TestGenerateECKey(t, elliptic.P256())
	return &TestSignerContext{
		Key:        key,
		CoseSigner: cose.NewTestCoseSigner(t, key),
	}
}

// SealedState signs a format-v3 checkpoint receipt committing to the provided
// (size, peaks) state, using the degenerate first-checkpoint proof shape
// (tree-size-1 = 0), and returns it decoded. Callers verifying against log
// data recover the accumulator from the massif as usual.
func (s *TestSignerContext) SealedState(
	logID storage.LogID, massifIndex uint64, state massifs.MMRState,
) (*massifs.Checkpoint, error) {
	_ = logID
	_ = massifIndex
	data, err := s.SignCheckpoint(state)
	if err != nil {
		return nil, err
	}
	checkpt, err := massifs.NewCheckpoint(data)
	if err != nil {
		return nil, err
	}
	return &checkpt, nil
}

// SignCheckpoint signs and encodes a format-v3 checkpoint receipt for the
// provided (size, peaks) state.
func (s *TestSignerContext) SignCheckpoint(state massifs.MMRState) ([]byte, error) {
	proof := massifs.ConsistencyProof{
		TreeSize1:  0,
		TreeSize2:  state.MMRSize,
		Paths:      [][][]byte{},
		RightPeaks: state.Peaks,
	}
	return massifs.SignCheckpointReceipt(s.CoseSigner, proof, state.Peaks)
}
