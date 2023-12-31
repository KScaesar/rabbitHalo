package rabbitHalo

import (
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
	mu         sync.Mutex
	minIndex   int
	usageQty   []int // key:value = { id : qty }
	totalQty   int
	currentLen int
	maxLen     int

	// 一定要指針
	// https://ithelp.ithome.com.tw/articles/10225968
	childUsageRate []*MinUsageRateStrategy
}

func (s *MinUsageRateStrategy) ViewUsageQty() (minIndex, totalScore int, listQty []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.viewUsageQty()
}

// viewUsageQty time is bigO( M * N * K * .... )
func (s *MinUsageRateStrategy) viewUsageQty() (minIndex, totalScore int, listQty []int) {
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
	totalScore = 0
	listQty = make([]int, s.maxLen)

	for i := 0; i < s.currentLen; i++ {
		child := s.childUsageRate[i]
		if child == nil {
			continue
		}

		_, childTotal, _ := child.viewUsageQty()

		// 必須把父子資源的分數一起計算
		// 如果只考慮子資源, 併發的時候
		// 子資源還沒被使用者申請, 因此父資源的分數都一樣
		// 會發生父資源大量搶奪同一個子資源
		// 最小 index 不會切換
		totalScore += childTotal + s.usageQty[i]
		score := childTotal + s.usageQty[i]
		listQty[i] = score
		if minValue > score {
			minValue = score
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
	s.mu.Lock()
	defer s.mu.Unlock()

	prev := s.minIndex
	s.usageQty[prev]++
	s.totalQty++

	if s.reachMaxLen() {
		s.minIndex = s.minimumIndex()
		return prev
	}

	nextIndex := roundRobinStrategy(s.minIndex, s.maxLen)
	s.minIndex = nextIndex
	s.currentLen++
	return prev
}

func (s *MinUsageRateStrategy) UpdateByRelease(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if id >= s.currentLen {
		panic("unknown the resource")
	}

	s.usageQty[id]--
	s.totalQty--

	s.minIndex = s.minimumIndex()
}

func (s *MinUsageRateStrategy) reachMaxLen() bool {
	return s.currentLen >= s.maxLen
}

func (s *MinUsageRateStrategy) notExistChildResource() bool {
	return s.childUsageRate == nil
}

func (s *MinUsageRateStrategy) InitChildStrategy(maxLen int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.childUsageRate != nil {
		return
	}
	s.childUsageRate = make([]*MinUsageRateStrategy, maxLen)
}

func (s *MinUsageRateStrategy) SetChildStrategy(id int, childStrategy *MinUsageRateStrategy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.childUsageRate[id] != nil {
		return
	}
	s.childUsageRate[id] = childStrategy
}
