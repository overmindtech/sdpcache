package v2

import (
	"math/rand"
	"testing"
	"time"

	"github.com/overmindtech/sdp-go"
)

const CacheDuration = 10 * time.Second

// NewPopulatedCache Returns a newly populated cache and the CacheQuery that
// matches a randomly selected item in that cache
func NewPopulatedCache(numberItems int) (*Cache, CacheQuery) {
	// Populate the cache
	c := NewCache()

	var item *sdp.Item
	var q CacheQuery
	exampleIndex := rand.Intn(numberItems)

	for i := 0; i < numberItems; i++ {
		item = GenerateRandomItem()

		if i == exampleIndex {
			q = ToCacheQuery(item)
		}

		c.StoreItem(item, CacheDuration)
	}

	return c, q
}

func BenchmarkCache1SingleItem(b *testing.B) {
	c, query := NewPopulatedCache(1)

	var err error

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Search for a single item
		_, err = c.Search(query)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCache10SingleItem(b *testing.B) {
	c, query := NewPopulatedCache(10)

	var err error

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Search for a single item
		_, err = c.Search(query)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCache100SingleItem(b *testing.B) {
	c, query := NewPopulatedCache(100)

	var err error

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Search for a single item
		_, err = c.Search(query)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCache1000SingleItem(b *testing.B) {
	c, query := NewPopulatedCache(1000)

	var err error

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Search for a single item
		_, err = c.Search(query)

		if err != nil {
			b.Fatal(err)
		}
	}
}
