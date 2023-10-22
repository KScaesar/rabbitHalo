package rabbitHalo

import (
	"log"
	"math"
	"sync"
)

func NewMinUsageRateStrategy(maxLen int) *MinUsageRateStrategy {
	return &MinUsageRateStrategy{
		usageQty: make([]int, maxLen),
		maxLen:   maxLen,
	}
}

type MinUsageRateStrategy struct {
	updateDataMutex sync.RWMutex
	searchMinMutex  sync.RWMutex
	minIndex        int
	usageQty        []int // key:value = { id : qty }
	totalQty        int
	currentLen      int
	maxLen          int

	// 一定要指針
	// https://ithelp.ithome.com.tw/articles/10225968
	childUsageRate []*MinUsageRateStrategy
}

func (s *MinUsageRateStrategy) ViewUsageQty() (minIndex, totalQty int, listQty []int) {
	s.updateDataMutex.Lock()
	defer s.updateDataMutex.Unlock()
	return s.viewUsageQty()
}

// viewUsageQty time is bigO( M * N * K * .... )
func (s *MinUsageRateStrategy) viewUsageQty() (minIndex, totalQty int, listQty []int) {
	if !s.reachMaxLen() || s.notExistChildResource() {
		minValue := math.MaxInt
		minIndex = 0
		for i, v := range s.usageQty {
			if minValue > v {
				minValue = v
				minIndex = i
			}
		}
		return minIndex, s.totalQty, s.usageQty
	}

	minValue := math.MaxInt
	minIndex = 0
	totalQty = 0
	listQty = make([]int, s.maxLen)

	for i := 0; i < s.currentLen; i++ {
		child := s.childUsageRate[i]
		if child == nil {
			continue
		}

		_, childTotal, _ := child.viewUsageQty()
		totalQty += childTotal
		listQty[i] = childTotal

		if minValue > childTotal {
			minValue = childTotal
			minIndex = i
		}
	}

	return
}

func (s *MinUsageRateStrategy) minimumIndex() int {
	minIndex, _, _ := s.viewUsageQty()
	return minIndex
}

func (s *MinUsageRateStrategy) UpdateByAcquire() (targetIndex int) {
	s.updateDataMutex.Lock()
	defer s.updateDataMutex.Unlock()

	prev := s.minIndex
	s.usageQty[prev]++
	s.totalQty++

	if s.reachMaxLen() {
		// s.searchMinMutex.Lock()
		s.minIndex = s.minimumIndex()
		// s.searchMinMutex.Unlock()
		return prev
	}

	next := roundRobinStrategy(s.minIndex, s.maxLen)
	s.minIndex = next
	s.currentLen++
	// fmt.Println("maxLen", s.maxLen, "len", s.currentLen, "prev", prev, "min", s.minIndex)
	return prev
}

func (s *MinUsageRateStrategy) UpdateByRelease(id int) {
	s.updateDataMutex.Lock()
	defer s.updateDataMutex.Unlock()

	if id >= s.currentLen {
		panic("unknown the resource")
	}

	s.usageQty[id]--
	s.totalQty--

	s.searchMinMutex.Lock()
	s.minIndex = s.minimumIndex()
	s.searchMinMutex.Unlock()
}

func (s *MinUsageRateStrategy) reachMaxLen() bool {
	return s.currentLen >= s.maxLen
}

func (s *MinUsageRateStrategy) notExistChildResource() bool {
	return s.childUsageRate == nil
}

// InitChildStrategy 初始化物件的時候呼叫, 所以不需要上鎖
func (s *MinUsageRateStrategy) InitChildStrategy(maxLen int) {
	s.childUsageRate = make([]*MinUsageRateStrategy, maxLen)
}

func (s *MinUsageRateStrategy) SetChildStrategy(id int, childStrategy *MinUsageRateStrategy) {
	s.childUsageRate[id] = childStrategy
}

func lazyNewResource[T any](strategy *MinUsageRateStrategy, resourceAll []T, factory func(id int) (T, error)) (T, error) {
	if strategy.reachMaxLen() { // 如果先更新後查詢, 狀態判斷會有錯誤
		log.Println("reuse resource")
		targetIndex := strategy.UpdateByAcquire()
		return resourceAll[targetIndex], nil
	}

	log.Println("lazy new")
	targetIndex := strategy.UpdateByAcquire()
	resource, err := factory(targetIndex)
	// fmt.Printf("lazy %#v\n", resource)
	if err != nil {
		return resource, err
	}
	resourceAll[targetIndex] = resource
	return resource, nil
}

func roundRobinStrategy(cursor int, maxLen int) (nextIndex int) {
	cursor++
	if cursor >= maxLen {
		// cursor = cursor % maxLen
		cursor = maxLen - 1
	}
	return cursor
}
