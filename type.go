package skiplist

import (
	"math/rand"
	"strconv"
	"sync"
)

type SkippedSequenceEntry struct {
	Start     uint64
	End       uint64
	Timestamp int64
}

// GetNumSequencesInEntry return the number of sequences in the SkippedSequenceEntry sequence range.
func (s *SkippedSequenceEntry) GetNumSequencesInEntry() int64 {
	if s.Start == s.End {
		return 1
	}
	return int64(s.End - s.Start + 1)
}

// String will return a string representation of the SkippedSequenceEntry
// Formats: Singular: "#<seq>" or ranges: "#<start>-#<end>"
func (s *SkippedSequenceEntry) String() string {
	seqStr := "#" + strconv.FormatUint(s.Start, 10)
	if s.End != 0 && s.End != s.Start {
		seqStr += "-#" + strconv.FormatUint(s.End, 10)
	}
	return seqStr
}

type elementNode struct {
	next []*Element
}

type Element struct {
	elementNode
	key SkippedSequenceEntry
}

// Key allows retrieval of the key for a given Element
func (e *Element) Key() SkippedSequenceEntry {
	return e.key
}

// Next returns the following Element or nil if we're at the end of the list.
// Only operates on the bottom level of the skip list (a fully linked list).
func (element *Element) Next() *Element {
	return element.next[0]
}

// IsWithinRange checks if the SkippedSequenceEntry is within the range of the Element's key.
func (element *Element) IsWithinRange(elem SkippedSequenceEntry) bool {
	return element.key.Start <= elem.Start && element.key.End >= elem.End
}

type SkipList struct {
	elementNode
	NumSequencesInList int64
	maxLevel           int
	Length             int
	randSource         rand.Source
	probability        float64
	probTable          []float64
	mutex              sync.RWMutex
	backElem           *Element
	prevNodesCache     []*elementNode
}

// GetLength will return the number of elements in the skiplist
func (list *SkipList) GetLength() int {
	list.mutex.RLock()
	defer list.mutex.RUnlock()
	return list.Length
}

// GetLastElement will return the last element in the skiplist
func (list *SkipList) GetLastElement() *Element {
	list.mutex.RLock()
	defer list.mutex.RUnlock()
	return list.backElem
}

// GetNumSequencesInList will return the total number of sequences in the skiplist
func (list *SkipList) GetNumSequencesInList() int64 {
	list.mutex.RLock()
	defer list.mutex.RUnlock()
	return list.NumSequencesInList
}
