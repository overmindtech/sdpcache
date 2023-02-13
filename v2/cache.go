package v2

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/btree"
	"github.com/overmindtech/sdp-go"
)

type IndexValues struct {
	SSTHash              SSTHash
	UniqueAttributeValue string
	Method               sdp.RequestMethod
	Query                string
}

type CacheQuery struct {
	SST                  SST // *required
	UniqueAttributeValue *string
	Method               *sdp.RequestMethod
	Query                *string
}

func (cc CacheQuery) String() string {
	fields := []string{
		("SourceName=" + cc.SST.SourceName),
		("Scope=" + cc.SST.Scope),
		("Type=" + cc.SST.Type),
	}

	if cc.UniqueAttributeValue != nil {
		fields = append(fields, ("UniqueAttributeValue=" + *cc.UniqueAttributeValue))
	}

	if cc.Method != nil {
		fields = append(fields, ("Method=" + cc.Method.String()))
	}

	if cc.Query != nil {
		fields = append(fields, ("Query=" + *cc.Query))
	}

	return strings.Join(fields, ", ")
}

// Matches Returns whether or not the supplied index values match the
// CacheQuery, excluding the SST since this will have already been validated.
// Note that this only checks values that ave actually been set in the
// CacheQuery
func (cc CacheQuery) Matches(i IndexValues) bool {
	// Check for any mismatches on the values that are set
	if cc.Method != nil {
		if *cc.Method != i.Method {
			return false
		}
	}

	if cc.Query != nil {
		if *cc.Query != i.Query {
			return false
		}
	}

	if cc.UniqueAttributeValue != nil {
		if *cc.UniqueAttributeValue != i.UniqueAttributeValue {
			return false
		}
	}

	return true
}

var ErrCacheNotFound = errors.New("not found in cache")

// SST A combination of SourceName, Scope and Type, all of which must be
// provided
type SST struct {
	SourceName string
	Scope      string
	Type       string
}

// Hash Creates a new SST hash from a given SST
func (s SST) Hash() SSTHash {
	h := sha1.New()
	h.Write([]byte(s.SourceName))
	h.Write([]byte(s.Scope))
	h.Write([]byte(s.Type))

	sum := make([]byte, 0)
	sum = h.Sum(sum)

	return SSTHash(fmt.Sprintf("%x", sum))
}

// CachedResult An item including cache metadata
type CachedResult struct {
	// Item is the actual cached item
	Item *sdp.Item

	// Error is the error that we want
	Error error

	// The time at which this item expires
	Expiry time.Time

	// Values that we use for calculating indexes
	IndexValues IndexValues
}

// SSTHash Represents the hash of `SourceName`, `Scope` and `Type`
type SSTHash string

type Cache struct {
	// Minimum amount of time to wait between cache purges
	MinWaitTime time.Duration

	// The timer that is used to trigger the next purge
	purgeTimer *time.Timer

	// The time that the purger will run next
	nextPurge time.Time

	indexes map[SSTHash]*indexSet

	// This index is used to track item expiries, since items can have different
	// expiry durations we need to use a btree here rather than just appending
	// to a slice or something. The purge process uses this to determine what
	// needs deleting, then calls into each specific index to delete as required
	expiryIndex *btree.BTreeG[*CachedResult]
}

func NewCache() *Cache {
	return &Cache{
		indexes: make(map[SSTHash]*indexSet),
		expiryIndex: btree.NewG(2, func(a, b *CachedResult) bool {
			return a.Expiry.Before(b.Expiry)
		}),
	}
}

type indexSet struct {
	uniqueAttributeValueIndex *btree.BTreeG[*CachedResult]
	methodIndex               *btree.BTreeG[*CachedResult]
	queryIndex                *btree.BTreeG[*CachedResult]
}

func newIndexSet() *indexSet {
	return &indexSet{
		uniqueAttributeValueIndex: btree.NewG(2, func(a, b *CachedResult) bool {
			return sortString(a.IndexValues.UniqueAttributeValue, a.Item) < sortString(b.IndexValues.UniqueAttributeValue, b.Item)
		}),
		methodIndex: btree.NewG(2, func(a, b *CachedResult) bool {
			return sortString(a.IndexValues.Method.String(), a.Item) < sortString(b.IndexValues.Method.String(), b.Item)
		}),
		queryIndex: btree.NewG(2, func(a, b *CachedResult) bool {
			return sortString(a.IndexValues.Query, a.Item) < sortString(b.IndexValues.Query, b.Item)
		}),
	}
}

// Search Runs a given query against the cache. If a cached error is found it
// will be returned immediately, if nothing is found a ErrCacheNotFound will
// be returned. Otherwise this will return items that match ALL of the given
// query parameters
func (c *Cache) Search(cc CacheQuery) ([]*sdp.Item, error) {
	items := make([]*sdp.Item, 0)
	var err error

	// Get the relevant set of indexes based on the SST Hash
	sstHash := cc.SST.Hash()
	indexes, exists := c.indexes[sstHash]
	pivot := CachedResult{
		IndexValues: IndexValues{
			SSTHash: sstHash,
		},
	}

	if !exists {
		// If we don't have a set of indexes then it definitely doesn't exist
		return items, ErrCacheNotFound
	}

	// Start with the most specific index and fall back to the least specific.
	// Checking all matching items and returning. These is no need to check all
	// indexes since they all have the same content
	if cc.UniqueAttributeValue != nil {
		pivot.IndexValues.UniqueAttributeValue = *cc.UniqueAttributeValue

		indexes.uniqueAttributeValueIndex.AscendGreaterOrEqual(&pivot, func(result *CachedResult) bool {
			if *cc.UniqueAttributeValue == result.IndexValues.UniqueAttributeValue {
				if cc.Matches(result.IndexValues) {
					if result.Error != nil {
						err = result.Error
					}

					if result.Item != nil {
						items = append(items, result.Item)
					}
				}

				// Always return true so that we continue to iterate
				return true
			}

			return false
		})

		if len(items) == 0 {
			return nil, ErrCacheNotFound
		}

		return items, err
	}

	if cc.Query != nil {
		pivot.IndexValues.Query = *cc.Query

		indexes.queryIndex.AscendGreaterOrEqual(&pivot, func(result *CachedResult) bool {
			if *cc.Query == result.IndexValues.Query {
				if cc.Matches(result.IndexValues) {
					if result.Error != nil {
						err = result.Error
					}

					if result.Item != nil {
						items = append(items, result.Item)
					}
				}

				// Always return true so that we continue to iterate
				return true
			}

			return false
		})

		if len(items) == 0 {
			return nil, ErrCacheNotFound
		}

		return items, err
	}

	if cc.Method != nil {
		pivot.IndexValues.Method = *cc.Method

		indexes.methodIndex.AscendGreaterOrEqual(&pivot, func(result *CachedResult) bool {
			if *cc.Method == result.IndexValues.Method {
				// If the methods match, check the rest
				if cc.Matches(result.IndexValues) {
					if result.Error != nil {
						err = result.Error
					}

					if result.Item != nil {
						items = append(items, result.Item)
					}
				}

				// Always return true so that we continue to iterate
				return true
			}

			return false
		})

		if len(items) == 0 {
			return nil, ErrCacheNotFound
		}

		return items, err
	}

	// If nothing other than SST has been set then return everything
	indexes.methodIndex.Ascend(func(result *CachedResult) bool {
		if result.Error != nil {
			err = result.Error
		}

		if result.Item != nil {
			items = append(items, result.Item)
		}

		return true
	})

	if len(items) == 0 {
		return nil, ErrCacheNotFound
	}

	return items, err
}

// StoreItem Stores an item in the cache. Note that this item must be fully
// populated (including metadata) for indexing to work correctly
func (c *Cache) StoreItem(item *sdp.Item, duration time.Duration) {
	if item == nil || c == nil {
		return
	}

	res := CachedResult{
		Item:   item,
		Error:  nil,
		Expiry: time.Now().Add(duration),
		IndexValues: IndexValues{
			UniqueAttributeValue: item.UniqueAttributeValue(),
			Method:               item.Metadata.SourceRequest.Method,
			Query:                item.Metadata.SourceRequest.Query,
			SSTHash: SST{
				SourceName: item.Metadata.SourceName,
				Scope:      item.Scope,
				Type:       item.Type,
			}.Hash(),
		},
	}

	c.storeResult(res)
}

// StoreError Stores an error for the given duration. Since we can't determine
// the index values from the error itself, the user also needs to pass these in.
func (c *Cache) StoreError(err error, duration time.Duration, indexValues IndexValues) {
	if c == nil || err == nil {
		return
	}

	res := CachedResult{
		Item:        nil,
		Error:       err,
		Expiry:      time.Now().Add(duration),
		IndexValues: indexValues,
	}

	c.storeResult(res)
}

func (c *Cache) storeResult(res CachedResult) {
	// Create the index if it doesn't exist
	indexes, ok := c.indexes[res.IndexValues.SSTHash]

	if !ok {
		indexes = newIndexSet()
		c.indexes[res.IndexValues.SSTHash] = indexes
	}

	// Add the item to the indexes
	indexes.methodIndex.ReplaceOrInsert(&res)
	indexes.queryIndex.ReplaceOrInsert(&res)
	indexes.uniqueAttributeValueIndex.ReplaceOrInsert(&res)

	// Add the item ot the expiry index
	c.expiryIndex.ReplaceOrInsert(&res)

	// Update the purge time if required
	if res.Expiry.Before(c.nextPurge) {
		c.setNextPurge(res.Expiry)
	}
}

// sortString Returns the string that the cached result should be sorted on.
// This has a prefix of the index value and suffix of the GloballyUniqueName if
// relevant
func sortString(indexValue string, item *sdp.Item) string {
	if item == nil {
		return indexValue
	} else {
		return indexValue + item.GloballyUniqueName()
	}
}

// PurgeStats Stats about the Purge
type PurgeStats struct {
	// How many items were timed out of the cache
	NumPurged int
	// How long the purging took overall
	TimeTaken time.Duration
	// The expiry time of the next item to expire. If there are no more items in
	// the cache, this will be nil
	NextExpiry *time.Time
}

// Purge Purges all expired items from the cache. The user must pass in the
// `before` time. All items that expired before this will be purged. Usually
// this would be just `time.Now()` however it could be overridden for testing
func (c *Cache) Purge(before time.Time) PurgeStats {
	// Store the current time rather than calling it a million times
	start := time.Now()

	var indexSet *indexSet
	var ok bool
	var nextExpiry *time.Time

	expired := make([]*CachedResult, 0)

	// Look through the expiry cache and work out what has expired
	c.expiryIndex.Ascend(func(res *CachedResult) bool {
		if res.Expiry.Before(before) {
			if indexSet, ok = c.indexes[res.IndexValues.SSTHash]; ok {
				// For each expired item, delete it from all of the indexes that it will be in
				if indexSet.methodIndex != nil {
					indexSet.methodIndex.Delete(res)
				}
				if indexSet.queryIndex != nil {
					indexSet.queryIndex.Delete(res)
				}
				if indexSet.uniqueAttributeValueIndex != nil {
					indexSet.uniqueAttributeValueIndex.Delete(res)
				}
			}

			expired = append(expired, res)

			return true
		}

		// Take note of the next expiry so we can schedule the next run
		nextExpiry = &res.Expiry

		// As soon as hit this we'll stop ascending
		return false
	})

	for _, r := range expired {
		c.expiryIndex.Delete(r)
	}

	return PurgeStats{
		NumPurged:  len(expired),
		TimeTaken:  time.Since(start),
		NextExpiry: nextExpiry,
	}
}

// MinWaitDefault The default minimum wait time
const MinWaitDefault = (5 * time.Second)

// GetMinWaitTime Returns the minimum wait time or the default if not set
func (c *Cache) GetMinWaitTime() time.Duration {
	if c.MinWaitTime == 0 {
		return MinWaitDefault
	}

	return c.MinWaitTime
}

// StartPurger Starts the purge process in the background, it will be cancelled
// when the context is cancelled. The cache will be purged initially, at which
// point the process will sleep until the next time an item expires
func (c *Cache) StartPurger(ctx context.Context) error {
	if c.purgeTimer == nil {
		c.purgeTimer = time.NewTimer(0)
	} else {
		return errors.New("purger already running")
	}

	go func(ctx context.Context) {
		for {
			select {
			case <-c.purgeTimer.C:
				stats := c.Purge(time.Now())

				if stats.NextExpiry == nil {
					// If there is nothing else in the cache, wait basically
					// forever
					c.purgeTimer.Reset(1000 * time.Hour)
					c.nextPurge = time.Now().Add(1000 * time.Hour)
				} else {
					if time.Until(*stats.NextExpiry) < c.GetMinWaitTime() {
						c.purgeTimer.Reset(c.GetMinWaitTime())
						c.nextPurge = time.Now().Add(c.GetMinWaitTime())
					} else {
						c.purgeTimer.Reset(time.Until(*stats.NextExpiry))
						c.nextPurge = *stats.NextExpiry
					}
				}
			case <-ctx.Done():
				c.purgeTimer.Stop()
				c.purgeTimer = nil
				return
			}
		}
	}(ctx)

	return nil
}

// setNextPurge Sets the next time the purger will run. While the purger is
// active this will be constantly updated, however if the purger is sleeping and
// new items are added this might need to be updated
func (c *Cache) setNextPurge(t time.Time) {
	if c.purgeTimer == nil {
		return
	}

	c.purgeTimer.Stop()
	c.nextPurge = t
	c.purgeTimer.Reset(time.Until(t))
}
