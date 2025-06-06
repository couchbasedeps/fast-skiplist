package skiplist

import (
	"fmt"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var benchList *SkipList
var discard *Element

func init() {
	// Initialize a big SkipList for the Get() benchmark
	benchList = New()

	for i := 0; i <= 10000000; i++ {
		_, err := benchList.Set(SkippedSequenceEntry{Start: uint64(i), End: uint64(i)})
		if err != nil {
			panic(fmt.Sprintf("failed to set element in skiplist: %v", err))
		}
	}

	// Display the sizes of our basic structs
	var sl SkipList
	var el Element
	fmt.Printf("Structure sizes: SkipList is %v, Element is %v bytes\n", unsafe.Sizeof(sl), unsafe.Sizeof(el))
}

func checkSanity(list *SkipList, t *testing.T) {
	// each level must be correctly ordered
	for k, v := range list.next {
		//t.Log("Level", k)

		if v == nil {
			continue
		}

		if k > len(v.next) {
			t.Fatal("first node's level must be no less than current level")
		}

		next := v
		cnt := 1

		for next.next[k] != nil {
			if !(next.next[k].key.Start >= next.key.End) {
				t.Fatalf("next key value must be greater than prev key value. [next:%v] [prev:%v]", next.next[k].key, next.key)
			}

			if k > len(next.next) {
				t.Fatalf("node's level must be no less than current level. [cur:%v] [node:%v]", k, next.next)
			}

			next = next.next[k]
			cnt++
		}

		if k == 0 {
			if cnt != list.Length {
				t.Fatalf("list len must match the level 0 nodes count. [cur:%v] [level0:%v]", cnt, list.Length)
			}
		}
	}
}

func TestBasicIntCRUD(t *testing.T) {
	var list *SkipList

	list = New()

	elem, err := list.Set(SkippedSequenceEntry{Start: 10, End: 10})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(10), elem.key.Start)
	assert.Equal(t, uint64(10), elem.key.End)
	elem, err = list.Set(SkippedSequenceEntry{Start: 60, End: 60})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(60), elem.key.Start)
	assert.Equal(t, uint64(60), elem.key.End)
	elem, err = list.Set(SkippedSequenceEntry{Start: 30, End: 31})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(30), elem.key.Start)
	assert.Equal(t, uint64(31), elem.key.End)
	elem, err = list.Set(SkippedSequenceEntry{Start: 20, End: 20})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(20), elem.key.Start)
	assert.Equal(t, uint64(20), elem.key.End)
	elem, err = list.Set(SkippedSequenceEntry{Start: 90, End: 90})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(90), elem.key.Start)
	assert.Equal(t, uint64(90), elem.key.End)
	checkSanity(list, t)

	// try set element that already exists in the list (30-31 exits so setting 30-30 should fail)
	elem, err = list.Set(SkippedSequenceEntry{Start: 30, End: 30})
	require.Error(t, err)
	require.Nil(t, elem)
	checkSanity(list, t)

	elem, err = list.Remove(SkippedSequenceEntry{Start: 0, End: 0})
	require.Error(t, err)
	require.Nil(t, elem)
	elem, err = list.Remove(SkippedSequenceEntry{Start: 20, End: 20})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(20), elem.key.Start)
	assert.Equal(t, uint64(20), elem.key.End)
	checkSanity(list, t)

	v1 := list.Get(SkippedSequenceEntry{Start: 10, End: 10})
	v2 := list.Get(SkippedSequenceEntry{Start: 60, End: 60})
	v3 := list.Get(SkippedSequenceEntry{Start: 30, End: 30})
	v4 := list.Get(SkippedSequenceEntry{Start: 20, End: 20})
	v5 := list.Get(SkippedSequenceEntry{Start: 90, End: 90})
	v6 := list.Get(SkippedSequenceEntry{Start: 0, End: 0})

	require.NotNil(t, v1)
	assert.Equal(t, uint64(10), v1.key.Start)
	assert.Equal(t, uint64(10), v1.key.End)

	require.NotNil(t, v2)
	assert.Equal(t, uint64(60), v2.key.Start)
	assert.Equal(t, uint64(60), v2.key.End)

	require.NotNil(t, v3)
	assert.Equal(t, uint64(30), v3.key.Start)
	assert.Equal(t, uint64(31), v3.key.End)

	require.Nil(t, v4)

	require.NotNil(t, v5)
	assert.Equal(t, uint64(90), v5.key.Start)
	assert.Equal(t, uint64(90), v5.key.End)

	require.Nil(t, v6)
}

func TestChangeLevel(t *testing.T) {
	var i uint64
	list := New()

	assert.Equal(t, DefaultMaxLevel, list.maxLevel)

	list = NewWithMaxLevel(4)
	assert.Equal(t, 4, list.maxLevel)

	for i = 1; i <= 201; i++ {
		elem, err := list.Set(SkippedSequenceEntry{Start: i * 10, End: i * 10})
		require.NoError(t, err)
		require.NotNil(t, elem)
	}

	checkSanity(list, t)

	if list.Length != 201 {
		t.Fatal("wrong list length", list.Length)
	}

	seq := uint64(1)
	for c := list.Front(); c != nil; c = c.Next() {
		cmp := seq * 10
		assert.Equal(t, cmp, c.key.Start)
		assert.Equal(t, cmp, c.key.End)
		seq++
	}
}

func TestMaxLevel(t *testing.T) {
	list := NewWithMaxLevel(DefaultMaxLevel + 1)
	elem, err := list.Set(SkippedSequenceEntry{Start: 0, End: 0})
	require.NoError(t, err)
	require.NotNil(t, elem)
}

func TestChangeProbability(t *testing.T) {
	list := New()

	if list.probability != DefaultProbability {
		t.Fatal("new lists should have P value = DefaultProbability")
	}

	list.SetProbability(0.5)
	if list.probability != 0.5 {
		t.Fatal("failed to set new list probability value: expected 0.5, got", list.probability)
	}
}

func TestConcurrency(t *testing.T) {
	list := New()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	elem, err := list.Set(SkippedSequenceEntry{Start: 1, End: 1})
	require.NoError(t, err)
	require.NotNil(t, elem)

	go func() {
		for i := 0; i < 100000; i++ {
			backSeq := list.backElem.Key().End
			elem, setErr := list.Set(SkippedSequenceEntry{Start: backSeq + 2, End: backSeq + 3})
			require.NoError(t, setErr)
			require.NotNil(t, elem)
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < 100000; i++ {
			backSeq := list.backElem.Key().End
			list.Get(SkippedSequenceEntry{Start: backSeq + 2, End: backSeq + 3})
		}
		wg.Done()
	}()

	wg.Wait()
	if list.Length != 100001 {
		fmt.Println("Length after concurrent operations:", list.Length)
		t.Fail()
	}
}

func BenchmarkIncSet(b *testing.B) {
	b.ReportAllocs()
	list := New()

	for i := 0; i < b.N; i++ {
		_, _ = list.Set(SkippedSequenceEntry{Start: uint64(i), End: uint64(i)})
	}

	b.SetBytes(int64(b.N))
}

func BenchmarkIncGet(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res := benchList.Get(SkippedSequenceEntry{Start: uint64(i), End: uint64(i)})
		if res == nil {
			b.Fatal("failed to Get an element that should exist")
		}
	}

	b.SetBytes(int64(b.N))
}

func BenchmarkDecSet(b *testing.B) {
	b.ReportAllocs()
	list := New()

	for i := b.N; i > 0; i-- {
		_, _ = list.Set(SkippedSequenceEntry{Start: uint64(i), End: uint64(i)})
	}

	b.SetBytes(int64(b.N))
}

func BenchmarkDecGet(b *testing.B) {
	b.ReportAllocs()
	for i := b.N; i > 0; i-- {
		res := benchList.Get(SkippedSequenceEntry{Start: uint64(i), End: uint64(i)})
		if res == nil {
			b.Fatal("failed to Get an element that should exist", i)
		}
	}

	b.SetBytes(int64(b.N))
}

func TestRemoveSeqFromRange(t *testing.T) {
	list := New()
	elem, err := list.Set(SkippedSequenceEntry{Start: 1, End: 5})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(1), elem.key.Start)
	assert.Equal(t, uint64(5), elem.key.End)
	elem, err = list.Set(SkippedSequenceEntry{Start: 8, End: 10})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(8), elem.key.Start)
	assert.Equal(t, uint64(10), elem.key.End)

	assert.Equal(t, int64(8), list.GetNumSequencesInList())

	// Remove a sequence from the first range
	elem, err = list.Remove(SkippedSequenceEntry{Start: 8, End: 8})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(8), elem.key.Start)
	assert.Equal(t, uint64(8), elem.key.End)

	assert.Equal(t, int64(7), list.GetNumSequencesInList())

	// Check if the first range is updated correctly
	elem = list.Get(SkippedSequenceEntry{Start: 8, End: 8})
	require.Nil(t, elem)

	elem = list.Get(SkippedSequenceEntry{Start: 9, End: 9})
	require.NotNil(t, elem)
	assert.Equal(t, uint64(9), elem.key.Start)
	assert.Equal(t, uint64(10), elem.key.End)

	elem, err = list.Remove(SkippedSequenceEntry{Start: 10, End: 10})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(10), elem.key.Start)
	assert.Equal(t, uint64(10), elem.key.End)

	assert.Equal(t, int64(6), list.GetNumSequencesInList())

	elem = list.Get(SkippedSequenceEntry{Start: 9, End: 9})
	require.NotNil(t, elem)
	assert.Equal(t, uint64(9), elem.key.Start)
	assert.Equal(t, uint64(9), elem.key.End)

	assert.Equal(t, 2, list.GetLength())

	elem, err = list.Remove(SkippedSequenceEntry{Start: 9, End: 9})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(9), elem.key.Start)
	assert.Equal(t, uint64(9), elem.key.End)

	assert.Equal(t, 1, list.GetLength())
	assert.Equal(t, int64(5), list.GetNumSequencesInList())

	elem, err = list.Remove(SkippedSequenceEntry{Start: 3, End: 3})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(3), elem.key.Start)
	assert.Equal(t, uint64(3), elem.key.End)

	assert.Equal(t, 2, list.GetLength())
	assert.Equal(t, int64(4), list.GetNumSequencesInList())

	elem = list.Get(SkippedSequenceEntry{Start: 1, End: 1})
	require.NotNil(t, elem)
	assert.Equal(t, uint64(1), elem.key.Start)
	assert.Equal(t, uint64(2), elem.key.End)

	elem = list.Get(SkippedSequenceEntry{Start: 4, End: 4})
	require.NotNil(t, elem)
	assert.Equal(t, uint64(4), elem.key.Start)
	assert.Equal(t, uint64(5), elem.key.End)

	// try get removed item
	elem = list.Get(SkippedSequenceEntry{Start: 3, End: 3})
	require.Nil(t, elem)
}

func TestRemoveFromThreeRange(t *testing.T) {
	list := New()
	elem, err := list.Set(SkippedSequenceEntry{Start: 1, End: 3})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(1), elem.key.Start)
	assert.Equal(t, uint64(3), elem.key.End)

	assert.Equal(t, 1, list.GetLength())
	assert.Equal(t, int64(3), list.GetNumSequencesInList())

	elem, err = list.Remove(SkippedSequenceEntry{Start: 2, End: 2})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(2), elem.key.Start)
	assert.Equal(t, uint64(2), elem.key.End)
	assert.Equal(t, int64(2), list.GetNumSequencesInList())

	elem = list.Get(SkippedSequenceEntry{Start: 1, End: 1})
	require.NotNil(t, elem)
	assert.Equal(t, uint64(1), elem.key.Start)
	assert.Equal(t, uint64(1), elem.key.End)

	elem = list.Get(SkippedSequenceEntry{Start: 3, End: 3})
	require.NotNil(t, elem)
	assert.Equal(t, uint64(3), elem.key.Start)
	assert.Equal(t, uint64(3), elem.key.End)

	elem = list.Get(SkippedSequenceEntry{Start: 2, End: 2})
	require.Nil(t, elem)

	assert.Equal(t, 2, list.GetLength())
}

func TestRemoveRange(t *testing.T) {
	list := New()
	elem, err := list.Set(SkippedSequenceEntry{Start: 1, End: 3})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(1), elem.key.Start)
	assert.Equal(t, uint64(3), elem.key.End)
	assert.Equal(t, int64(3), list.GetNumSequencesInList())

	elem, err = list.Remove(SkippedSequenceEntry{Start: 1, End: 3})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(1), elem.key.Start)
	assert.Equal(t, uint64(3), elem.key.End)
	assert.Equal(t, int64(0), list.GetNumSequencesInList())

	elem, err = list.Set(SkippedSequenceEntry{Start: 1, End: 10})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(1), elem.key.Start)
	assert.Equal(t, uint64(10), elem.key.End)
	assert.Equal(t, int64(10), list.GetNumSequencesInList())

	elem, err = list.Remove(SkippedSequenceEntry{Start: 1, End: 5})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(1), elem.key.Start)
	assert.Equal(t, uint64(5), elem.key.End)
	assert.Equal(t, int64(5), list.GetNumSequencesInList())

	elem = list.Get(SkippedSequenceEntry{Start: 8, End: 8})
	require.NotNil(t, elem)
	assert.Equal(t, uint64(6), elem.key.Start)
	assert.Equal(t, uint64(10), elem.key.End)

	elem = list.Get(SkippedSequenceEntry{Start: 1, End: 1})
	require.Nil(t, elem)
}

func TestGetFrontElem(t *testing.T) {
	list := New()

	elem := list.Front()
	if elem != nil {
		t.Fatal("Front element should be nil")
	}

	// all items below are being added are contiguous so will extend the same element in the list
	elem, err := list.Set(SkippedSequenceEntry{Start: 1, End: 1})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(1), elem.key.Start)
	assert.Equal(t, uint64(1), elem.key.End)
	elem, err = list.Set(SkippedSequenceEntry{Start: 2, End: 2})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(1), elem.key.Start)
	assert.Equal(t, uint64(2), elem.key.End)
	elem, err = list.Set(SkippedSequenceEntry{Start: 3, End: 3})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(1), elem.key.Start)
	assert.Equal(t, uint64(3), elem.key.End)

	elem = list.Front()
	require.NotNil(t, elem)
	assert.Equal(t, uint64(1), elem.key.Start)
	assert.Equal(t, uint64(3), elem.key.End)
}

func TestRemoveRangeAcrossElements(t *testing.T) {
	list := New()

	elem, err := list.Set(SkippedSequenceEntry{Start: 1, End: 3})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(1), elem.key.Start)
	assert.Equal(t, uint64(3), elem.key.End)
	elem, err = list.Set(SkippedSequenceEntry{Start: 5, End: 6})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(5), elem.key.Start)
	assert.Equal(t, uint64(6), elem.key.End)

	assert.Equal(t, 2, list.GetLength())
	assert.Equal(t, int64(5), list.GetNumSequencesInList())

	elem, err = list.Remove(SkippedSequenceEntry{Start: 1, End: 5})
	require.NoError(t, err)
	require.NotNil(t, elem)

	assert.Equal(t, 1, list.GetLength())
	assert.Equal(t, int64(1), list.GetNumSequencesInList())

	elem = list.Get(SkippedSequenceEntry{Start: 1, End: 1})
	require.Nil(t, elem)

	elem = list.Get(SkippedSequenceEntry{Start: 6, End: 6})
	require.NotNil(t, elem)
	assert.Equal(t, uint64(6), elem.key.Start)
	assert.Equal(t, uint64(6), elem.key.End)

	elem, err = list.Set(SkippedSequenceEntry{Start: 8, End: 8})
	require.NoError(t, err)
	require.NotNil(t, elem)
	elem, err = list.Set(SkippedSequenceEntry{Start: 10, End: 10})
	require.NoError(t, err)
	require.NotNil(t, elem)

	assert.Equal(t, 3, list.GetLength())
	assert.Equal(t, int64(3), list.GetNumSequencesInList())

	elem, err = list.Remove(SkippedSequenceEntry{Start: 6, End: 10})
	require.NoError(t, err)
	require.NotNil(t, elem)

	elem = list.Get(SkippedSequenceEntry{Start: 8, End: 8})
	require.Nil(t, elem)
	elem = list.Get(SkippedSequenceEntry{Start: 10, End: 10})
	require.Nil(t, elem)

	assert.Equal(t, 0, list.GetLength())
	assert.Equal(t, int64(0), list.GetNumSequencesInList())

	elem, err = list.Set(SkippedSequenceEntry{Start: 1, End: 3})
	require.NoError(t, err)
	require.NotNil(t, elem)
	elem, err = list.Set(SkippedSequenceEntry{Start: 5, End: 6})
	require.NoError(t, err)
	require.NotNil(t, elem)

	elem, err = list.Remove(SkippedSequenceEntry{Start: 2, End: 6})
	require.NoError(t, err)
	require.NotNil(t, elem)

	assert.Equal(t, 1, list.GetLength())
	assert.Equal(t, int64(1), list.GetNumSequencesInList())
}

func TestCompact(t *testing.T) {
	list := New()

	elem, err := list.Set(SkippedSequenceEntry{Start: 1, End: 3, Timestamp: time.Now().Unix() - 1000})
	require.NoError(t, err)
	require.NotNil(t, elem)
	elem, err = list.Set(SkippedSequenceEntry{Start: 4, End: 6, Timestamp: time.Now().Unix() - 1000})
	require.NoError(t, err)
	require.NotNil(t, elem)

	num := list.CompactList(time.Now().Unix(), 100)
	assert.Equal(t, int64(6), num)
	assert.Equal(t, 0, list.GetLength())
	assert.Equal(t, int64(0), list.GetNumSequencesInList())

	assert.Nil(t, list.backElem)

	num = list.CompactList(time.Now().Unix(), 100)
	assert.Equal(t, int64(0), num)
	assert.Equal(t, 0, list.GetLength())
	assert.Equal(t, int64(0), list.GetNumSequencesInList())
}

func TestRemovingFromLastElem(t *testing.T) {
	list := New()

	elem, err := list.Set(SkippedSequenceEntry{Start: 1, End: 3, Timestamp: 0})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(1), elem.key.Start)
	assert.Equal(t, uint64(3), elem.key.End)

	assert.Equal(t, 1, list.GetLength())
	assert.Equal(t, uint64(1), list.backElem.key.Start)
	assert.Equal(t, uint64(3), list.backElem.key.End)
	assert.Equal(t, int64(3), list.GetNumSequencesInList())

	elem, err = list.Remove(SkippedSequenceEntry{Start: 1, End: 3})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(1), elem.key.Start)
	assert.Equal(t, uint64(3), elem.key.End)
	assert.Equal(t, 0, list.GetLength())
	assert.Nil(t, list.backElem)

	assert.Equal(t, int64(0), list.GetNumSequencesInList())
	assert.Equal(t, 0, list.GetLength())

	// add elem back
	elem, err = list.Set(SkippedSequenceEntry{Start: 1, End: 3, Timestamp: 0})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(1), elem.key.Start)
	assert.Equal(t, uint64(3), elem.key.End)

	// remove subset
	elem, err = list.Remove(SkippedSequenceEntry{Start: 2, End: 2})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(2), elem.key.Start)
	assert.Equal(t, uint64(2), elem.key.End)
	assert.Equal(t, 2, list.GetLength())
	assert.Equal(t, uint64(3), list.backElem.key.Start)
	assert.Equal(t, uint64(3), list.backElem.key.End)
	assert.Equal(t, int64(2), list.GetNumSequencesInList())
}

func TestRemoveItemNotInList(t *testing.T) {
	list := New()

	// remove item form empty list
	elem, err := list.Remove(SkippedSequenceEntry{Start: 1, End: 1})
	require.Error(t, err)
	assert.ErrorContains(t, err, "skiplist empty")
	require.Nil(t, elem, "Expected nil when removing from an empty list")

	elem, err = list.Set(SkippedSequenceEntry{Start: 1, End: 3})
	require.NoError(t, err)
	require.NotNil(t, elem)
	assert.Equal(t, uint64(1), elem.key.Start)
	assert.Equal(t, uint64(3), elem.key.End)

	elem, err = list.Remove(SkippedSequenceEntry{Start: 4, End: 4})
	require.Error(t, err)
	require.Nil(t, elem, "Expected nil when removing an item not in the list")
}
