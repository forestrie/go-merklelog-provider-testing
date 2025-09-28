package mmrtesting

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/datatrails/go-datatrails-common-api-gen/assets/v2/assets"
	"github.com/datatrails/go-datatrails-common-api-gen/attribute/v2/attribute"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/datatrails/go-datatrails-simplehash/simplehash"
	"github.com/robinbryce/go-merklelog-azure/datatrails"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// resourceChangedProperty            = "resource_changed"
	// resourceChangeMerkleLogStoredEvent = "assetsv2merklelogeventstored"
	names               = 2
	assetAttributeWords = 4
	eventAttributeWords = 6
	// DefaultCheckpointIssuer = "https://github.com/robinbryce/testing/checkpoint-issuer/default"
)

func NewDataTrailsLeafGenerator(g *TestGenerator, logID storage.LogID) LeafGenerator {
	leafGenerator := LeafGenerator{
		LogID: logID,
		Generator: func(logID storage.LogID, base, i uint64) any {
			return g.DataTrailsGenerateLeafContent(logID, base, i)
		},
		Encoder: func(a any) AddLeafArgs {
			return g.DataTrailsEncodeLeafForAddition(a)
		},
	}
	return leafGenerator
}

func (g *TestGenerator) DataTrailsGenerateLeafContent(logID storage.LogID, base, i uint64) any {

	assetIdentity := assets.AssetIdentityFromUuid(g.NewRandomUUIDString(g.T))
	attrValPrefix := ""
	tenantIdentity := datatrails.Log2TenantID(logID)
	assetUUID := strings.Split(assetIdentity, "/")[1]

	name := strings.Join(g.WordList(names), "")
	email := fmt.Sprintf("%s@datatrails.com", name)
	subject := strconv.Itoa(g.Rand.Intn(math.MaxInt))

	// Use the desired event rate as the upper bound, and generate a time stamp at lastTime + rand(0, upper-bound * 2)
	// So the generated event stream will be around the target rate.
	ts := g.SinceLastJitter()

	event := &assets.EventResponse{
		Identity:      g.NewEventIdentity(assetUUID),
		AssetIdentity: assetIdentity,
		EventAttributes: map[string]*attribute.Attribute{
			"forestrie.testGenerator-sequence-number": {
				Value: &attribute.Attribute_StrVal{
					StrVal: strconv.Itoa(int(base + i)),
				},
			},
			"forestrie.testGenerator-label": {
				Value: &attribute.Attribute_StrVal{
					StrVal: fmt.Sprintf("%s%s", attrValPrefix, "GenerateNextEvent"),
				},
			},

			"event-attribute-0": {
				Value: &attribute.Attribute_StrVal{
					StrVal: g.MultiWordString(eventAttributeWords),
				},
			},
		},
		AssetAttributes: map[string]*attribute.Attribute{
			"asset-attribute-0": {
				Value: &attribute.Attribute_StrVal{
					StrVal: g.MultiWordString(assetAttributeWords),
				},
			},
		},
		Operation: "Record",
		// Behavior:          "RecordEvidence",
		TimestampDeclared:  timestamppb.New(ts),
		TimestampAccepted:  timestamppb.New(ts),
		TimestampCommitted: nil,
		PrincipalDeclared: &assets.Principal{
			Issuer:      "https://rkvt.com",
			Subject:     subject,
			DisplayName: name,
			Email:       email,
		},
		PrincipalAccepted: &assets.Principal{
			Issuer:      "https://rkvt.com",
			Subject:     subject,
			DisplayName: name,
			Email:       email,
		},
		ConfirmationStatus: assets.ConfirmationStatus_PENDING,
		From:               "0xf8dfc073650503aeD429E414bE7e972f8F095e70",
		// TenantIdentity:     "tenant/0684984b-654d-4301-ad10-a508126e187d",
		TenantIdentity: tenantIdentity,
	}
	g.LastTime = ts

	return event

}
func (g *TestGenerator) DataTrailsEncodeLeafForAddition(a any) AddLeafArgs {

	LeafTypePlain := uint8(0)
	event, ok := a.(*assets.EventResponse)
	require.True(g.T, ok)
	// ID TIMSTAMP COMMITMENT
	id, err := g.NextID()
	require.NoError(g.T, err)

	logID := datatrails.TenantID2LogID(event.TenantIdentity)

	hasher := simplehash.NewHasherV3()
	hasher.HashEvent(
		event,
		// domain separation, default is LeafTypePlain (0)
		simplehash.WithPrefix([]byte{uint8(LeafTypePlain)}),
		simplehash.WithIDCommitted(id),
	)

	return AddLeafArgs{
		ID:    id,
		AppID: []byte(event.GetIdentity()),
		Value: hasher.Sum(nil),
		LogID: logID,
	}
}

func (g *TestGenerator) NewEventIdentity(assetUUID string) string {
	return assets.EventIdentityFromUuid(assetUUID, g.NewRandomUUIDString(g.T))
}

func (g *TestGenerator) NewAssetIdentity() string {
	return assets.AssetIdentityFromUuid(g.NewRandomUUIDString(g.T))
}
