package rabbitHalo

import (
	"time"

	"github.com/cenkalti/backoff/v4"
)

func retry(retryMaxTime time.Duration, fnName string, fn func() error) error {
	cfg := backoff.NewExponentialBackOff()
	cfg.InitialInterval = 500 * time.Millisecond
	cfg.Multiplier = 1.5
	cfg.RandomizationFactor = 0.5
	cfg.MaxElapsedTime = retryMaxTime

	cnt := 0
	logger := defaultLogger.WithCallDepth(7) // 連第三方套件的函數呼叫深度, 也要計算進去, 因為是 closure function
	countWrapper := func() error {
		logger.Info("%v: retry %v times", fnName, cnt)
		cnt++
		return fn()
	}

	return backoff.Retry(countWrapper, cfg)
}

func lazyNewResource[T any](strategy *MinUsageRateStrategy, resourceAll []T, factory func(id int) (T, error)) (T, error) {
	if strategy.reachMaxLen() { // 如果先更新後查詢, 狀態判斷會有錯誤
		targetIndex := strategy.UpdateByAcquire()
		return resourceAll[targetIndex], nil
	}

	targetIndex := strategy.UpdateByAcquire()
	resource, err := factory(targetIndex)
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

func GetMessageId(msg *AmqpMessage) string {
	msgId, ok := msg.Headers["msg_id"].(string)
	if !ok {
		msgId = ""
	}
	return msgId
}
