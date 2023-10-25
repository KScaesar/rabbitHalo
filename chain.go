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
