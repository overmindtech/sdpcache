package sdpcache

import (
	"math/rand"
	"time"

	"github.com/overmindtech/sdp-go"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var Types = []string{
	"person",
	"dog",
	"kite",
	"flag",
	"cat",
	"leopard",
	"fish",
	"bird",
	"kangaroo",
	"ostrich",
	"emu",
	"hawk",
	"mole",
	"badger",
	"lemur",
}

const MaxAttributes = 30
const MaxAttributeKeyLength = 20
const MaxAttributeValueLength = 50
const MaxLinkedItems = 10
const MaxLinkedItemRequests = 10

// GenerateRandomItem Generates a random item and the tags for this item. The
// tags include the name, type and a tag called "all" with a value of "all"
func GenerateRandomItem() (*sdp.Item, Tags) {
	attrs := make(map[string]interface{})

	name := randSeq(rand.Intn(MaxAttributeValueLength))
	typ := Types[rand.Intn(len(Types))]
	scope := randSeq(rand.Intn(MaxAttributeKeyLength))
	attrs["name"] = name

	for i := 0; i < rand.Intn(MaxAttributes); i++ {
		attrs[randSeq(rand.Intn(MaxAttributeKeyLength))] = randSeq(rand.Intn(MaxAttributeValueLength))
	}

	attributes, _ := sdp.ToAttributes(attrs)

	linkedItems := make([]*sdp.Reference, rand.Intn(MaxLinkedItems))

	for i := range linkedItems {
		linkedItems[i] = &sdp.Reference{
			Type:                 randSeq(rand.Intn(MaxAttributeKeyLength)),
			UniqueAttributeValue: randSeq(rand.Intn(MaxAttributeValueLength)),
			Scope:                randSeq(rand.Intn(MaxAttributeKeyLength)),
		}
	}

	linkedItemRequests := make([]*sdp.ItemRequest, rand.Intn(MaxLinkedItemRequests))

	for i := range linkedItemRequests {
		linkedItemRequests[i] = &sdp.ItemRequest{
			Type:      randSeq(rand.Intn(MaxAttributeKeyLength)),
			Method:    sdp.RequestMethod(rand.Intn(3)),
			Query:     randSeq(rand.Intn(MaxAttributeValueLength)),
			LinkDepth: rand.Uint32(),
			Scope:     randSeq(rand.Intn(MaxAttributeKeyLength)),
		}
	}

	item := sdp.Item{
		Type:               typ,
		UniqueAttribute:    "name",
		Attributes:         attributes,
		Scope:              scope,
		LinkedItemRequests: linkedItemRequests,
		LinkedItems:        linkedItems,
		Metadata: &sdp.Metadata{
			SourceName: randSeq(rand.Intn(MaxAttributeKeyLength)),
			SourceRequest: &sdp.ItemRequest{
				Type:      typ,
				Method:    sdp.RequestMethod_GET,
				Query:     name,
				LinkDepth: 1,
				Scope:     scope,
			},
			Timestamp:             timestamppb.New(time.Now()),
			SourceDuration:        durationpb.New(time.Millisecond * time.Duration(rand.Int63())),
			SourceDurationPerItem: durationpb.New(time.Millisecond * time.Duration(rand.Int63())),
		},
	}

	tags := Tags{
		"name": name,
		"type": typ,
		"all":  "all",
		"foo":  "bar",
		"bar":  "baz",
		"yes":  "yes",
		"no":   "no",
	}

	return &item, tags
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
