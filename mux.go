package rabbitHalo

import (
	"context"
	"sync"
)

func NewMessageMux(muxSize ...int) *MessageMux {
	var size int
	if len(muxSize) == 0 {
		const defaultMuxSize = 10
		size = defaultMuxSize
	} else {
		size = muxSize[0]
	}

	mux := &MessageMux{
		consumerHandlers:             make(map[string]ConsumerFunc, size),
		consumerHandlersByDynamicKey: make([]ConsumerFunc, 0, size),
	}
	return mux
}

type MessageMux struct {
	mu                           sync.RWMutex
	consumerHandlers             map[string]ConsumerFunc // RoutingKey:Func
	consumerHandlersByDynamicKey []ConsumerFunc
	consumerChain                ConsumerChain
}

func (mux *MessageMux) ServeConsume(ctx context.Context, msg *AmqpMessage) error {
	return handleTechnicalContext(mux.serveConsume)(ctx, msg)
}

func (mux *MessageMux) serveConsume(ctx context.Context, msg *AmqpMessage) error {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	key := msg.RoutingKey
	handler, ok := mux.consumerHandlers[key]
	if ok {
		return mux.consumerChain.Link(handler)(ctx, msg)
	}

	for _, fn := range mux.consumerHandlersByDynamicKey {
		err := mux.consumerChain.Link(fn)(ctx, msg)
		if err != nil || isStoppedNext(ctx) {
			return err
		}
	}

	notMatchKeyHandler := func(_ context.Context, _ *AmqpMessage) error { return ErrNotMatchRoutingKey }
	return mux.consumerChain.LinkError(notMatchKeyHandler)(ctx, msg)
}

// AddConsumerFeatureChain
// The execution order of chain can be referenced
// in the chain_test.go: TestConsumerChain_Link_confirm_the_execution_order_of_decorators
//
// The first decorator in the chain,
// 'before' will be executed first, and then 'after' will be executed last.
//
//	chain[0] = func(next ConsumerFunc) ConsumerFunc {
//	       return func(defaultCtx context.Context, msg *AmqpMessage) error {
//	           // before
//	           err := next(defaultCtx, msg)
//	           // after
//	           return err
//	       }
//	   }
func (mux *MessageMux) AddConsumerFeatureChain(chain ...ConsumerDecorator) *MessageMux {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	mux.consumerChain.AddFeature(chain...)
	return mux
}

func (mux *MessageMux) AddConsumerErrorChain(chain ...ConsumerDecorator) *MessageMux {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	mux.consumerChain.AddError(chain...)
	return mux
}

func (mux *MessageMux) RegisterConsumerFunc(key AmqpKey, fn ConsumerFunc) {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	routingKey := key.RoutingKey()
	if routingKey == "" {
		panic("RoutingKey is empty")
	}
	if routingKey == DynamicRoutingKey {
		panic("not allow DynamicRoutingKey")
	}
	_, ok := mux.consumerHandlers[routingKey]
	if ok {
		panic("duplicate key: " + routingKey)
	}

	mux.consumerHandlers[routingKey] = fn
}

func (mux *MessageMux) RemoveConsumerFunc(key AmqpKey) {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	delete(mux.consumerHandlers, key.RoutingKey())
}

// RegisterConsumerFuncByDynamic
// 使用場景:
// 無法在伺服器啟動當下決定的 key
func (mux *MessageMux) RegisterConsumerFuncByDynamic(key AmqpKey, fn ConsumerFunc) (index int) {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	routingKey := key.RoutingKey()
	if routingKey == "" {
		panic("RoutingKey is empty")
	}
	if routingKey != DynamicRoutingKey {
		panic("routingKey must is DynamicRoutingKey")
	}

	n := len(mux.consumerHandlersByDynamicKey)
	mux.consumerHandlersByDynamicKey = append(mux.consumerHandlersByDynamicKey, fn)
	return n
}

// RemoveConsumerFuncByDynamic
// The idx corresponds to the return value of RegisterConsumerFuncByDynamic.
func (mux *MessageMux) RemoveConsumerFuncByDynamic(idx int) {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	if idx >= len(mux.consumerHandlersByDynamicKey) {
		return
	}
	mux.consumerHandlersByDynamicKey = append(mux.consumerHandlersByDynamicKey[:idx], mux.consumerHandlersByDynamicKey[idx+1:]...)
}

//

func handleDefaultConsumerError(next ConsumerFunc) ConsumerFunc {
	return func(ctx context.Context, msg *AmqpMessage) error {
		err := next(ctx, msg)
		if err == nil {
			return nil
		}

		switch {
		default:
			msg.Ack(false)
			defaultLogger.Error("default error handle: msgType=%q: consumer=%q: key=%q: payload=%q: %v",
				msg.Type,
				msg.ConsumerTag,
				msg.RoutingKey,
				msg.Body,
				err,
			)
			return err
		}
	}
}

func handleTechnicalContext(next ConsumerFunc) ConsumerFunc {
	return func(ctx context.Context, msg *AmqpMessage) error {
		ctx1 := contextWithNextChecker(ctx)
		// ctx2 := ctx1
		lastCtx := ctx1
		return next(lastCtx, msg)
	}
}

//

type nextChecker struct{}

func contextWithNextChecker(ctx context.Context) context.Context {
	var isStopped bool
	return context.WithValue(ctx, nextChecker{}, &isStopped)
}

func isStoppedNext(ctx context.Context) bool {
	isStopped := ctx.Value(nextChecker{}).(*bool)
	return *isStopped
}

// MuxStopNext
// 與 XXXByDynamic 相關函數一起使用, 由 handler 進行確認是否符合 key
//
// 如果單獨使用 handler, 沒有 mux 註冊
// 不需要使用 MuxStopNext
func MuxStopNext(ctx context.Context) {
	isStopped := ctx.Value(nextChecker{}).(*bool)
	*isStopped = true
}
