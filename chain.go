package rabbitHalo

import (
	"context"
)

type ConsumerFunc func(ctx context.Context, msg *AmqpMessage) error

type ConsumerDecorator func(ConsumerFunc) ConsumerFunc

func NewConsumerChain(err, feat []ConsumerDecorator) *ConsumerChain {
	return &ConsumerChain{
		errorChain:   err,
		featureChain: feat,
	}
}

type ConsumerChain struct {
	errorChain   []ConsumerDecorator
	featureChain []ConsumerDecorator
}

func (c *ConsumerChain) LinkAll(fnAll []ConsumerFunc, isStop func() bool) ConsumerFunc {
	featureFn := func(ctx context.Context, msg *AmqpMessage) (Err error) {
		for _, fn := range fnAll {
			// 即使遇到錯誤也不停止, 直到符合條件
			err := c.LinkFeature(fn)(ctx, msg)
			if isStop() {
				return err
			}
			if err != nil {
				Err = err
			}
		}
		// 如果所有消費者都不滿足停止條件, 只記錄最後一個錯誤
		return
	}

	// 為了解決多個 handler 發生錯誤
	// amqp.ack 可能被執行多次
	// 所以單獨把錯誤處理額外執行
	return c.LinkError(featureFn)
}

// Link 執行順序參考範例 https://go.dev/play/p/I9cK-VvKjzi
func (c *ConsumerChain) Link(fn ConsumerFunc) ConsumerFunc {
	if len(c.errorChain) == 0 {
		c.errorChain = []ConsumerDecorator{handleDefaultConsumerError}
	}
	all := make([]ConsumerDecorator, 0, len(c.errorChain)+len(c.featureChain))
	all = append(all, c.errorChain...)
	all = append(all, c.featureChain...)
	return LinkFuncAndChain(fn, all...)
}

func (c *ConsumerChain) LinkError(fn ConsumerFunc) ConsumerFunc {
	if len(c.errorChain) == 0 {
		c.errorChain = []ConsumerDecorator{handleDefaultConsumerError}
	}
	return LinkFuncAndChain(fn, c.errorChain...)
}

func (c *ConsumerChain) LinkFeature(fn ConsumerFunc) ConsumerFunc {
	return LinkFuncAndChain(fn, c.featureChain...)
}

func LinkFuncAndChain(handler ConsumerFunc, chain ...ConsumerDecorator) ConsumerFunc {
	n := len(chain)
	for i := n - 1; 0 <= i; i-- {
		decorator := chain[i]
		handler = decorator(handler)
	}
	return handler
}

func (c *ConsumerChain) AddError(all ...ConsumerDecorator) *ConsumerChain {
	c.errorChain = append(c.errorChain, all...)
	return c
}

func (c *ConsumerChain) AddFeature(all ...ConsumerDecorator) *ConsumerChain {
	c.featureChain = append(c.featureChain, all...)
	return c
}

func (c *ConsumerChain) SetError(all ...ConsumerDecorator) *ConsumerChain {
	c.errorChain = all
	return c
}

func (c *ConsumerChain) SetFeature(all ...ConsumerDecorator) *ConsumerChain {
	c.featureChain = all
	return c
}

func (c *ConsumerChain) ErrorChain() []ConsumerDecorator {
	target := make([]ConsumerDecorator, len(c.errorChain))
	copy(target, c.errorChain)
	return target
}

func (c *ConsumerChain) FeatureChain() []ConsumerDecorator {
	target := make([]ConsumerDecorator, len(c.featureChain))
	copy(target, c.featureChain)
	return target
}
