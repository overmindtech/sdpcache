package sdpcache

import (
	"testing"
	"time"

	"github.com/overmindtech/sdp-go"
)

const CacheDuration = 10 * time.Second

// NewPopulatedCache Returns a newly populated cache and the tags of the first
// item inserted
func NewPopulatedCache(name string, numberItems int) (*Cache, Tags) {
	// Populate the cache
	c := Cache{
		Name:        name,
		MinWaitTime: 50 * time.Millisecond,
	}

	var item *sdp.Item
	var tags Tags
	var searchTags Tags

	for i := 0; i < numberItems; i++ {
		item, tags = GenerateRandomItem()

		if i == 0 {
			searchTags = tags
		}

		c.StoreItem(item, CacheDuration, tags)
	}

	return &c, searchTags
}

func BenchmarkCache1SingleItem(b *testing.B) {
	c, tags := NewPopulatedCache(b.Name(), 1)

	var err error

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Search for a single item
		_, err = c.Search(tags)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCache10SingleItem(b *testing.B) {
	c, tags := NewPopulatedCache(b.Name(), 10)

	var err error

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Search for a single item
		_, err = c.Search(tags)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCache100SingleItem(b *testing.B) {
	c, tags := NewPopulatedCache(b.Name(), 100)

	var err error

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Search for a single item
		_, err = c.Search(tags)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCache1000SingleItem(b *testing.B) {
	c, tags := NewPopulatedCache(b.Name(), 1000)

	var err error

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Search for a single item
		_, err = c.Search(tags)

		if err != nil {
			b.Fatal(err)
		}
	}
}

var AllItemTags = Tags{
	"all": "all",
}

func BenchmarkCache1AllItem(b *testing.B) {
	c, _ := NewPopulatedCache(b.Name(), 1)

	var err error

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Search for all items
		_, err = c.Search(AllItemTags)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCache10AllItem(b *testing.B) {
	c, _ := NewPopulatedCache(b.Name(), 10)

	var err error

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Search for all items
		_, err = c.Search(AllItemTags)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCache100AllItem(b *testing.B) {
	c, _ := NewPopulatedCache(b.Name(), 100)

	var err error

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Search for all items
		_, err = c.Search(AllItemTags)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCache1000AllItem(b *testing.B) {
	c, _ := NewPopulatedCache(b.Name(), 1000)

	var err error

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Search for all items
		_, err = c.Search(AllItemTags)

		if err != nil {
			b.Fatal(err)
		}
	}
}

var ManyTags = Tags{
	"all": "all",
	"foo": "bar",
	"bar": "baz",
	"yes": "yes",
	"no":  "no",
}

func BenchmarkCache1AllItemManyTags(b *testing.B) {
	c, _ := NewPopulatedCache(b.Name(), 1)

	var err error

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Search for all items
		_, err = c.Search(ManyTags)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCache10AllItemManyTags(b *testing.B) {
	c, _ := NewPopulatedCache(b.Name(), 10)

	var err error

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Search for all items
		_, err = c.Search(ManyTags)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCache100AllItemManyTags(b *testing.B) {
	c, _ := NewPopulatedCache(b.Name(), 100)

	var err error

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Search for all items
		_, err = c.Search(ManyTags)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCache1000AllItemManyTags(b *testing.B) {
	c, _ := NewPopulatedCache(b.Name(), 1000)

	var err error

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Search for all items
		_, err = c.Search(ManyTags)

		if err != nil {
			b.Fatal(err)
		}
	}
}
