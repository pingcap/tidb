package infoschema

import (
	"container/list"
	"context"
	// "github.com/scalalang2/golang-fifo/types"
	"sync"
	"time"
)

type Sizer interface {
	Size() int
}

const numberOfBuckets = 100

// entry holds the key and value of a cache entry.
type entry[K comparable, V Sizer] struct {
	key       K
	value     V
	visited   bool
	element   *list.Element
	expiredAt time.Time
	bucketID  int8 // bucketID is an index which the entry is stored in the bucket
}

// bucket is a container holding entries to be expired
type bucket[K comparable, V Sizer] struct {
	entries     map[K]*entry[K, V]
	newestEntry time.Time
}

type Sieve[K comparable, V Sizer] struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	size   int
	items  map[K]*entry[K, V]
	ll     *list.List
	hand   *list.Element

	buckets []bucket[K, V]

	// ttl is the time to live of the cache entry
	ttl        time.Duration
	ttlEnabled bool

	// nextCleanupBucket is an index of the next bucket to be cleaned up
	nextCleanupBucket int8

	// callback is the function that will be called when an entry is evicted from the cache
	// callback types.OnEvictCallback[K, V]
}

// var _ types.Cache[int, int] = (*Sieve[int, int])(nil)

func New[K comparable, V Sizer](size int, ttl time.Duration) *Sieve[K, V] {
	ctx, cancel := context.WithCancel(context.Background())

	if ttl <= 0 {
		ttl = 0
	}

	cache := &Sieve[K, V]{
		ctx:               ctx,
		cancel:            cancel,
		size:              size,
		items:             make(map[K]*entry[K, V]),
		ll:                list.New(),
		buckets:           make([]bucket[K, V], numberOfBuckets),
		ttl:               ttl,
		nextCleanupBucket: 0,
	}

	for i := 0; i < numberOfBuckets; i++ {
		cache.buckets[i].entries = make(map[K]*entry[K, V])
	}

	// if ttl != 0 {
	// 	go func(ctx context.Context) {
	// 		ticker := time.NewTicker(ttl / numberOfBuckets)
	// 		defer ticker.Stop()
	// 		for {
	// 			select {
	// 			case <-ctx.Done():
	// 				return
	// 			case <-ticker.C:
	// 				cache.deleteExpired()
	// 			}
	// 		}
	// 	}(cache.ctx)
	// }

	return cache
}

func (s *Sieve[K, V]) Set(key K, Sizer V) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.items[key]; ok {
		s.removeFromBucket(e) // remove from the bucket as the entry is updated
		e.value = value
		e.visited = true
		e.expiredAt = time.Now().Add(s.ttl)
		s.addToBucket(e)
		return
	}

	if s.ll.Len() >= s.size {
		s.evict()
	}

	e := &entry[K, V]{
		key:       key,
		value:     value,
		element:   s.ll.PushFront(key),
		expiredAt: time.Now().Add(s.ttl),
	}
	s.items[key] = e
	s.addToBucket(e)
}

func (s *Sieve[K, V]) Get(key K) (value V, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.items[key]; ok {
		e.visited = true
		return e.value, true
	}

	return
}

func (s *Sieve[K, V]) Remove(key K) (ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.items[key]; ok {
		// if the element to be removed is the hand,
		// then move the hand to the previous one.
		if e.element == s.hand {
			s.hand = s.hand.Prev()
		}

		s.removeEntry(e)
		return true
	}

	return false
}

func (s *Sieve[K, V]) Contains(key K) (ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok = s.items[key]
	return
}

func (s *Sieve[K, V]) Peek(key K) (value V, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.items[key]; ok {
		return e.value, true
	}

	return
}

// func (s *Sieve[K, V]) SetOnEvicted(callback types.OnEvictCallback[K, V]) {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()

// 	s.callback = callback
// }

func (s *Sieve[K, V]) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.ll.Len()
}

func (s *Sieve[K, V]) Purge() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, e := range s.items {
		s.removeEntry(e)
	}

	for i := range s.buckets {
		for k := range s.buckets[i].entries {
			delete(s.buckets[i].entries, k)
		}
	}

	s.ll.Init()
}

func (s *Sieve[K, V]) Close() {
	s.Purge()
	s.mu.Lock()
	s.cancel()
	s.mu.Unlock()
}

func (s *Sieve[K, V]) removeEntry(e *entry[K, V]) {
	if s.callback != nil {
		s.callback(e.key, e.value)
	}

	s.ll.Remove(e.element)
	s.removeFromBucket(e)
	delete(s.items, e.key)
}

func (s *Sieve[K, V]) evict() {
	o := s.hand
	// if o is nil, then assign it to the tail element in the list
	if o == nil {
		o = s.ll.Back()
	}

	el, ok := s.items[o.Value.(K)]
	if !ok {
		panic("sieve: evicting non-existent element")
	}

	for el.visited {
		el.visited = false
		o = o.Prev()
		if o == nil {
			o = s.ll.Back()
		}

		el, ok = s.items[o.Value.(K)]
		if !ok {
			panic("sieve: evicting non-existent element")
		}
	}

	s.hand = o.Prev()
	s.removeEntry(el)
}

func (s *Sieve[K, V]) addToBucket(e *entry[K, V]) {
	if s.ttl == 0 {
		return
	}
	bucketId := (numberOfBuckets + s.nextCleanupBucket - 1) % numberOfBuckets
	e.bucketID = bucketId
	s.buckets[bucketId].entries[e.key] = e
	if s.buckets[bucketId].newestEntry.Before(e.expiredAt) {
		s.buckets[bucketId].newestEntry = e.expiredAt
	}
}

func (s *Sieve[K, V]) removeFromBucket(e *entry[K, V]) {
	if s.ttl == 0 {
		return
	}
	delete(s.buckets[e.bucketID].entries, e.key)
}

// func (s *Sieve[K, V]) deleteExpired() {
// 	s.mu.Lock()

// 	bucketId := s.nextCleanupBucket
// 	s.nextCleanupBucket = (s.nextCleanupBucket + 1) % numberOfBuckets
// 	bucket := &s.buckets[bucketId]
// 	timeToExpire := time.Until(bucket.newestEntry)
// 	if timeToExpire > 0 {
// 		s.mu.Unlock()
// 		time.Sleep(timeToExpire)
// 		s.mu.Lock()
// 	}

// 	for _, e := range bucket.entries {
// 		s.removeEntry(e)
// 	}

// 	s.mu.Unlock()
// }

// Of returns the size of 'v' in bytes.
// If there is an error during calculation, Of returns -1.
func Of(v interface{}) int {
       // Cache with every visited pointer so we don't count two pointers
       // to the same memory twice.
       cache := make(map[uintptr]bool)
       return sizeOf(reflect.Indirect(reflect.ValueOf(v)), cache, 0)
}

// sizeOf returns the number of bytes the actual data represented by v occupies in memory.
// If there is an error, sizeOf returns -1.
func sizeOf(v reflect.Value, cache map[uintptr]bool, depth int) int {

       switch v.Kind() {

       case reflect.Array:
               sum := 0
               for i := 0; i < v.Len(); i++ {
                       s := sizeOf(v.Index(i), cache, depth+1)
                       if s < 0 {
                               return -1
                       }
                       sum += s
               }

               return sum + (v.Cap()-v.Len())*int(v.Type().Elem().Size())

       case reflect.Slice:
               // return 0 if this node has been visited already
               if cache[v.Pointer()] {
                       return 0
               }
               cache[v.Pointer()] = true

               sum := 0
               for i := 0; i < v.Len(); i++ {
                       s := sizeOf(v.Index(i), cache, depth+1)
                       if s < 0 {
                               return -1
                       }
                       sum += s
               }

               sum += (v.Cap() - v.Len()) * int(v.Type().Elem().Size())

               return sum + int(v.Type().Size())

       case reflect.Struct:

               // fmt.Println("handling struct type ;;;;;;;;;", v.Type())

               sum := 0
               for i, n := 0, v.NumField(); i < n; i++ {
                       s := sizeOf(v.Field(i), cache, depth+1)
                       // for kk :=0; kk < depth; kk++ {
                       //      fmt.Print("\t")
                       // }
                       // fmt.Printf("%dfield%d size:%d\n", depth, i, s)
                       if s < 0 {
                               return -1
                       }
                       sum += s
               }

               // Look for struct padding.
               padding := int(v.Type().Size())
               for i, n := 0, v.NumField(); i < n; i++ {
                       padding -= int(v.Field(i).Type().Size())
               }

               return sum + padding

       case reflect.String:
               s := v.String()
               hdr := (*reflect.StringHeader)(unsafe.Pointer(&s))
               if cache[hdr.Data] {
                       return int(v.Type().Size())
               }
               cache[hdr.Data] = true
               return len(s) + int(v.Type().Size())

       case reflect.Ptr:
               // return Ptr size if this node has been visited already (infinite recursion)
               if cache[v.Pointer()] {
                       return int(v.Type().Size())
               }
               cache[v.Pointer()] = true
               if v.IsNil() {
                       return int(reflect.New(v.Type()).Type().Size())
               }
               s := sizeOf(reflect.Indirect(v), cache, depth+1)
               if s < 0 {
                       return -1
               }
               return s + int(v.Type().Size())

       case reflect.Bool,
               reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
               reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
               reflect.Int, reflect.Uint,
               reflect.Chan,
               reflect.Uintptr,
               reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128,
               reflect.Func:
               return int(v.Type().Size())

       case reflect.Map:
               // return 0 if this node has been visited already (infinite recursion)
               if cache[v.Pointer()] {
                       return 0
               }
               cache[v.Pointer()] = true
               sum := 0
               keys := v.MapKeys()
               for i := range keys {
                       val := v.MapIndex(keys[i])
                       // calculate size of key and value separately
                       sv := sizeOf(val, cache, depth+1)
                       if sv < 0 {
                               return -1
                       }
                       sum += sv
                       sk := sizeOf(keys[i], cache, depth+1)
                       if sk < 0 {
                               return -1
                       }
                       sum += sk
               }
               // Include overhead due to unused map buckets.  10.79 comes
               // from https://golang.org/src/runtime/map.go.
               return sum + int(v.Type().Size()) + int(float64(len(keys))*10.79)

       case reflect.Interface:

               fmt.Println("sizeof:", v.Type().Name())
               switch v.Type().Name() {
               case "Storage", "Client":
                       return 8
               }

               return sizeOf(v.Elem(), cache, depth+1) + int(v.Type().Size())

       case reflect.UnsafePointer:
               s := int(v.Type().Size())
               fmt.Println("unsafe pointer, size ", s)
               // return -1
               return s

       }

       fmt.Println("=== unknown reflect type ==", v.Kind())

       // return 0

       return -1
}

func TestXXX(t *testing.T) {
       store, dom := testkit.CreateMockStoreAndDomain(t)
       tk := testkit.NewTestKit(t, store)
       tk.MustExec("use test")
       tk.MustExec("create table t (id int)")
       tk.MustExec("create table pt (id int) partition by hash(id) partitions 1024")

       is := dom.InfoSchema()
       tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
       require.NoError(t, err)

       pt, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("pt"))
       require.NoError(t, err)

       sizeT, err := getRealSizeOf(tb)
       fmt.Println("table size =", sizeT, err)

       sizeT, err = getRealSizeOf(pt)
       fmt.Println("partition table size =", sizeT, err)

       fmt.Println("==============11==", Of(tb))
       fmt.Println("==============22==", Of(pt))
       fmt.Println("==============33==", Of(pt.GetPartitionedTable()))
       ptt := pt.GetPartitionedTable()
       fmt.Println("==============44==", Of(ptt.GetPartition(ptt.GetAllPartitionIDs()[0])))
}
