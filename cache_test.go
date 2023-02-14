package sdpcache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/overmindtech/sdp-go"
)

func TestStoreItem(t *testing.T) {
	cache := NewCache()

	t.Run("one match", func(t *testing.T) {
		item := GenerateRandomItem()
		cache.StoreItem(item, 10*time.Second)

		results, err := cache.Search(ToCacheQuery(item))

		if err != nil {
			t.Error(err)
		}

		if len(results) != 1 {
			t.Errorf("expected 1 result, got %v", len(results))
		}
	})

	t.Run("another match", func(t *testing.T) {
		item := GenerateRandomItem()
		cache.StoreItem(item, 10*time.Second)

		results, err := cache.Search(ToCacheQuery(item))

		if err != nil {
			t.Error(err)
		}

		if len(results) != 1 {
			t.Errorf("expected 1 result, got %v", len(results))
		}
	})
}

func TestStoreError(t *testing.T) {
	cache := NewCache()

	t.Run("with just an error", func(t *testing.T) {
		sst := SST{
			SourceName: "foo",
			Scope:      "foo",
			Type:       "foo",
		}

		uav := "foo"

		cache.StoreError(errors.New("arse"), 10*time.Second, IndexValues{
			SSTHash: sst.Hash(),
			Method:  sdp.RequestMethod_GET,
			Query:   uav,
		})

		items, err := cache.Search(CacheQuery{
			SST:    sst,
			Method: sdp.RequestMethod_GET.Enum(),
			Query:  &uav,
		})

		if len(items) > 0 {
			t.Errorf("expected 0 items, got %v", len(items))
		}

		if err == nil {
			t.Error("expected error, got nil")
		}
	})

	t.Run("with items and an error for the same query", func(t *testing.T) {
		// Add an item with the same details as above
		item := GenerateRandomItem()
		item.Metadata.SourceRequest.Method = sdp.RequestMethod_GET
		item.Metadata.SourceRequest.Query = "foo"
		item.Metadata.SourceName = "foo"
		item.Scope = "foo"
		item.Type = "foo"

		items, err := cache.Search(CacheQuery{
			SST: SST{
				SourceName: item.Metadata.SourceName,
				Scope:      item.Scope,
				Type:       item.Type,
			},
			Method: &item.Metadata.SourceRequest.Method,
			Query:  &item.Metadata.SourceRequest.Query,
		})

		if len(items) > 0 {
			t.Errorf("expected 0 items, got %v", len(items))
		}

		if err == nil {
			t.Error("expected error, got nil")
		}
	})

	t.Run("with multiple errors", func(t *testing.T) {
		sst := SST{
			SourceName: "foo",
			Scope:      "foo",
			Type:       "foo",
		}

		uav := "foo"

		cache.StoreError(errors.New("nope"), 10*time.Second, IndexValues{
			SSTHash: sst.Hash(),
			Method:  sdp.RequestMethod_GET,
			Query:   uav,
		})

		items, err := cache.Search(CacheQuery{
			SST:    sst,
			Method: sdp.RequestMethod_GET.Enum(),
			Query:  &uav,
		})

		if len(items) > 0 {
			t.Errorf("expected 0 items, got %v", len(items))
		}

		if err == nil {
			t.Error("expected error, got nil")
		}
	})
}

func ToCacheQuery(item *sdp.Item) CacheQuery {
	uav := item.UniqueAttributeValue()

	return CacheQuery{
		SST: SST{
			SourceName: item.Metadata.SourceName,
			Scope:      item.Scope,
			Type:       item.Type,
		},
		UniqueAttributeValue: &uav,
		Method:               &item.Metadata.SourceRequest.Method,
		Query:                &item.Metadata.SourceRequest.Query,
	}
}

func TestPurge(t *testing.T) {
	cache := NewCache()

	cachedItems := []struct {
		Item   *sdp.Item
		Expiry time.Time
	}{
		{
			Item:   GenerateRandomItem(),
			Expiry: time.Now().Add(0 * time.Second),
		},
		{
			Item:   GenerateRandomItem(),
			Expiry: time.Now().Add(1 * time.Second),
		},
		{
			Item:   GenerateRandomItem(),
			Expiry: time.Now().Add(2 * time.Second),
		},
		{
			Item:   GenerateRandomItem(),
			Expiry: time.Now().Add(3 * time.Second),
		},
		{
			Item:   GenerateRandomItem(),
			Expiry: time.Now().Add(4 * time.Second),
		},
		{
			Item:   GenerateRandomItem(),
			Expiry: time.Now().Add(5 * time.Second),
		},
	}

	for _, i := range cachedItems {
		cache.StoreItem(i.Item, time.Until(i.Expiry))
	}

	// Make sure all the items are in the cache
	for _, i := range cachedItems {
		uav := i.Item.UniqueAttributeValue()
		items, err := cache.Search(CacheQuery{
			SST: SST{
				SourceName: i.Item.Metadata.SourceName,
				Scope:      i.Item.Scope,
				Type:       i.Item.Type,
			},
			UniqueAttributeValue: &uav,
			Method:               &i.Item.Metadata.SourceRequest.Method,
			Query:                &i.Item.Metadata.SourceRequest.Query,
		})

		if err != nil {
			t.Error(err)
		}

		if len(items) != 1 {
			t.Errorf("expected 1 item, got %v", len(items))
		}
	}

	// Purge just the first one
	stats := cache.Purge(cachedItems[0].Expiry.Add(500 * time.Millisecond))

	if stats.NumPurged != 1 {
		t.Errorf("expected 1 item purged, got %v", stats.NumPurged)
	}

	// The times won't be exactly equal because we're checking it against
	// time.Now more than once. So I need to check that they are *almost* the
	// same, but not exactly
	nextExpiryString := stats.NextExpiry.Format(time.RFC3339)
	expectedNextExpiryString := cachedItems[1].Expiry.Format(time.RFC3339)

	if nextExpiryString != expectedNextExpiryString {
		t.Errorf("expected next expiry to be %v, got %v", expectedNextExpiryString, nextExpiryString)
	}

	// Purge all but the last one
	stats = cache.Purge(cachedItems[4].Expiry.Add(500 * time.Millisecond))

	if stats.NumPurged != 4 {
		t.Errorf("expected 4 item purged, got %v", stats.NumPurged)
	}

	// Purge the last one
	stats = cache.Purge(cachedItems[5].Expiry.Add(500 * time.Millisecond))

	if stats.NumPurged != 1 {
		t.Errorf("expected 1 item purged, got %v", stats.NumPurged)
	}

	if stats.NextExpiry != nil {
		t.Errorf("expected expiry to be nil, got %v", stats.NextExpiry)
	}
}

func TestStartPurge(t *testing.T) {
	cache := NewCache()
	cache.MinWaitTime = 100 * time.Millisecond

	cachedItems := []struct {
		Item   *sdp.Item
		Expiry time.Time
	}{
		{
			Item:   GenerateRandomItem(),
			Expiry: time.Now().Add(0),
		},
		{
			Item:   GenerateRandomItem(),
			Expiry: time.Now().Add(100 * time.Millisecond),
		},
	}

	for _, i := range cachedItems {
		cache.StoreItem(i.Item, time.Until(i.Expiry))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := cache.StartPurger(ctx)

	if err != nil {
		t.Error(err)
	}

	// Wait for everything to be purged
	time.Sleep(200 * time.Millisecond)

	// At this point everything should be been cleaned, and the purger should be
	// sleeping forever
	items, err := cache.Search(ToCacheQuery(cachedItems[1].Item))

	if !errors.Is(err, ErrCacheNotFound) {
		t.Errorf("unexpected error: %v", err)
		t.Errorf("unexpected items: %v", len(items))
	}

	cache.purgeMutex.Lock()
	if cache.nextPurge.Before(time.Now().Add(time.Hour)) {
		// If the next purge is within the next hour that's an error, it should
		// be really, really for in the future
		t.Errorf("Expected next purge to be in 1000 years, got %v", cache.nextPurge.String())
	}
	cache.purgeMutex.Unlock()

	// Adding a new item should kick off the purging again
	for _, i := range cachedItems {
		cache.StoreItem(i.Item, 100*time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond)

	// It should be empty again
	items, err = cache.Search(ToCacheQuery(cachedItems[1].Item))

	if !errors.Is(err, ErrCacheNotFound) {
		t.Errorf("unexpected error: %v", err)
		t.Errorf("unexpected items: %v", len(items))
	}
}

func TestStopPurge(t *testing.T) {
	cache := NewCache()
	cache.MinWaitTime = 1 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())

	cache.StartPurger(ctx)

	// Stop the purger
	cancel()

	// Insert an item
	item := GenerateRandomItem()
	cache.StoreItem(item, time.Millisecond)
	sst := SST{
		SourceName: item.Metadata.SourceName,
		Scope:      item.Scope,
		Type:       item.Type,
	}

	// Make sure it's not purged
	time.Sleep(100 * time.Millisecond)
	items, err := cache.Search(CacheQuery{
		SST: sst,
	})

	if err != nil {
		t.Error(err)
	}

	if len(items) != 1 {
		t.Errorf("Expected 1 item, got %v", len(items))
	}
}

func TestDelete(t *testing.T) {
	cache := NewCache()

	// Insert an item
	item := GenerateRandomItem()
	cache.StoreItem(item, time.Millisecond)
	sst := SST{
		SourceName: item.Metadata.SourceName,
		Scope:      item.Scope,
		Type:       item.Type,
	}

	// It should be there
	items, err := cache.Search(CacheQuery{
		SST: sst,
	})

	if err != nil {
		t.Error(err)
	}

	if len(items) != 1 {
		t.Errorf("expected 1 item, got %v", len(items))
	}

	// Delete it
	cache.Delete(CacheQuery{
		SST: sst,
	})

	// It should be gone
	items, err = cache.Search(CacheQuery{
		SST: sst,
	})

	if err != ErrCacheNotFound {
		t.Errorf("expected ErrCacheNotFound, got %v", err)
	}

	if len(items) != 0 {
		t.Errorf("expected 0 item, got %v", len(items))
	}
}

// This test is designed to be run with -race to ensure that there aren't any
// data races
func TestConcurrent(t *testing.T) {
	cache := NewCache()
	// Run the purger super fast to generate a worst-case scenario
	cache.MinWaitTime = 1 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache.StartPurger(ctx)

	var wg sync.WaitGroup

	numParallel := 1_000

	for i := 0; i < numParallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Store the item
			item := GenerateRandomItem()
			cache.StoreItem(item, 100*time.Millisecond)

			wg.Add(1)
			// Create a goroutine to also delete in parallel
			go func() {
				defer wg.Done()
				cache.Delete(ToCacheQuery(item))
			}()
		}()
	}

	wg.Wait()
}

func TestPointers(t *testing.T) {
	cache := NewCache()

	item := GenerateRandomItem()

	cache.StoreItem(item, time.Minute)
	cq := ToCacheQuery(item)

	item.Type = "bad"

	items, err := cache.Search(cq)

	if err != nil {
		t.Error(err)
	}

	if len(items) != 1 {
		t.Errorf("expected 1 item, got %v", len(items))
	}

	if items[0].Type == "bad" {
		t.Error("item was changed in cache")
	}
}
