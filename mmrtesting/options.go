// Package mmrtesting provides shared test support for merklelog's
package mmrtesting

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"fmt"
	"math/rand"
	"testing"

	commoncbor "github.com/datatrails/go-datatrails-merklelog/massifs/cbor"
	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/veraison/go-cose"
)

// TestOptions holds options generic for all storage implementations.
type TestOptions struct {
	massifs.SignerOptions
	CBORCodec           commoncbor.CBORCodec
	COSEVerifier        cose.Verifier
	DefaultMassifHeight uint8
	// We seed the RNG of the provided StartTimeMS. It is normal to force it to
	// some fixed value so that the generated data is the same from run to run.
	StartTimeMS     int64
	EventRate       int
	TestLabelPrefix string
	Rand            *rand.Rand
	WordList        []string // used for generating random words, defaults to bip32WordList

	CheckpointIssuer string
	DisableSigning   bool // if true, the signer will not sign anything
	LogLevel         string
	RootDir          string

	errs []error // set not nil if there was an error processing options
}

// WithCommitmentEpoch sets the CommitmentEpoch option for TestOptions.

func WithNoSigning() massifs.Option {
	return func(o any) {
		options, ok := o.(*TestOptions)
		if !ok {
			return
		}
		options.DisableSigning = true
	}
}

func WithCheckpointIssuer(issuer string) massifs.Option {
	return func(o any) {
		options, ok := o.(*TestOptions)
		if !ok {
			return
		}
		options.CheckpointIssuer = issuer
	}
}

// WithStartTimeMS sets the StartTimeMS option for TestOptions.  This option
// determines the seed for the random number generator used in tests.  As with
// any option that should pre-empt the defaults,it must be placed before
// WithDefaults to take effect.
func WithStartTimeMS(startTimeMS int64) massifs.Option {
	return func(o any) {
		options, ok := o.(*TestOptions)
		if !ok {
			return
		}
		options.StartTimeMS = startTimeMS
	}
}

// WithTestLabelPrefix pre-empts how the tests are identified. it is also
// typically used to isolate storage for integration tests
func WithTestLabelPrefix(prefix string) massifs.Option {
	return func(o any) {
		options, ok := o.(*TestOptions)
		if !ok {
			return
		}
		options.TestLabelPrefix = prefix
	}
}

func (o *TestOptions) EnsureDefaults(t *testing.T) {

	WithDefaults()(o)
	if len(o.errs) > 0 {
		t.Fatalf("failed to initialize test options: %v", o.errs[0])
	}
	if o.RootDir == "" {
		o.RootDir = t.TempDir()
	}
}

// WithDefaults can be used to populate test options with defaults during option processing.
func WithDefaults() massifs.Option {
	return func(o any) {
		options, ok := o.(*TestOptions)
		if !ok {
			return
		}
		logLevel := options.LogLevel
		if logLevel == "" {
			logLevel = "NOOP"
			options.LogLevel = logLevel
		}

		// default leaf type plain is the zero value.
		if options.CheckpointIssuer == "" {
			options.CheckpointIssuer = DefaultCheckpointIssuer
		}

		if options.DefaultMassifHeight == 0 {
			options.DefaultMassifHeight = 14 // production default
		}

		if options.StartTimeMS == 0 {
			options.StartTimeMS = (1698342521) * 1000
		}

		if options.Rand == nil {
			options.Rand = rand.New(rand.NewSource(options.StartTimeMS / 1000))
		}
		if options.WordList == nil {
			options.WordList = bip32WordList()
		}

		if options.EventRate == 0 {
			options.EventRate = 500 // arbitrary default
		}
		if options.TestLabelPrefix == "" {
			a := options.WordList[options.Rand.Intn(len(options.WordList))]
			b := options.WordList[options.Rand.Intn(len(options.WordList))]
			options.TestLabelPrefix = fmt.Sprintf("mmrtesting.%s-%s", a, b)
		}

		if !options.DisableSigning && options.Signer == nil {
			if options.Key == nil {
				privateKey, err := ecdsa.GenerateKey(elliptic.P256(), options.Rand)
				if err != nil {
					options.errs = append(options.errs, fmt.Errorf("failed to generate private key: %w", err))
					return
				}
				options.Key = privateKey
				options.PubKey = &privateKey.PublicKey
				options.Signer, err = cose.NewSigner(cose.AlgorithmES256, options.Key)
				if err != nil {
					options.errs = append(options.errs, fmt.Errorf("failed to create signer: %w", err))
				}
			} else {
				var err error
				options.PubKey = &options.Key.PublicKey
				options.Signer, err = cose.NewSigner(options.Alg, options.Key)
				if err != nil {
					options.errs = append(options.errs, fmt.Errorf("failed to create signer: %w", err))
				}
			}
		}
	}
}
