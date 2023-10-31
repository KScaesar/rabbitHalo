package rabbitHalo

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
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
		cNormalHandlers:         make(map[string]ConsumerFunc, size),
		cFanoutHandlers:         make(map[string]ConsumerFunc, size),
		cBlankKeyFanoutHandlers: make([]ConsumerFunc, 0, size),
		cWildcardTopicHandlers:  make([]ConsumerFunc, 0, size),
	}
	return mux
}

type MessageMux struct {
	mu              sync.RWMutex
	isGoroutineSafe atomic.Bool

	// Due to the highly flexible RoutingKey mechanism in RabbitMQ,
	// RoutingKey and BindingKey may not be an exact match.
	//
	// However, this mux (multiplexer) can only achieve "string exact" comparison,
	// and it relies on handler functions to compare keys.
	// Utilizing the Chain of Responsibility Pattern and StopNextFunc, it seeks the appropriate handler.
	//
	// The following RoutingKey searching efficiency is low,
	// and it may be considered to not use mux to register handler functions.
	//
	// 1. For topic kind, which include wildcards like *.*.*
	// 2. For fanout kind, RabbitMQ allows matching any format of RoutingKey.
	cNormalHandlers         map[string]ConsumerFunc // RoutingKey:Func
	cFanoutHandlers         map[string]ConsumerFunc // RoutingKey:Func
	cBlankKeyFanoutHandlers []ConsumerFunc
	cWildcardTopicHandlers  []ConsumerFunc
	consumerChain           ConsumerChain
}

func (mux *MessageMux) ServeConsume(ctx context.Context, msg *AmqpMessage) error {
	return handleTechnicalContext(mux.serveConsume)(ctx, msg)
}

func (mux *MessageMux) serveConsume(ctx context.Context, msg *AmqpMessage) error {
	if mux.isGoroutineSafe.Load() {
		mux.mu.Lock()
		defer mux.mu.Unlock()
	}
	notMatchKeyHandler := func(_ context.Context, _ *AmqpMessage) error { return ErrNotMatchRoutingKey }

	routingKey := msg.RoutingKey
	handler, ok := mux.cNormalHandlers[routingKey]
	if ok {
		return mux.consumerChain.Link(handler)(ctx, msg)
	}

	if routingKey == "" {
		for _, fanoutHandler := range mux.cBlankKeyFanoutHandlers {
			err := mux.consumerChain.Link(fanoutHandler)(ctx, msg)
			if err != nil || isNextFuncStopped(ctx) {
				return err
			}
		}
		return mux.consumerChain.LinkError(notMatchKeyHandler)(ctx, msg)
	}

	if fanoutHandler, exist := mux.cFanoutHandlers[routingKey]; exist {
		return mux.consumerChain.Link(fanoutHandler)(ctx, msg)
	}

	for _, topicHandler := range mux.cWildcardTopicHandlers {
		err := mux.consumerChain.Link(topicHandler)(ctx, msg)
		if err != nil || isNextFuncStopped(ctx) {
			return err
		}
	}

	for _, fanoutHandler := range mux.cBlankKeyFanoutHandlers {
		err := mux.consumerChain.Link(fanoutHandler)(ctx, msg)
		if err != nil || isNextFuncStopped(ctx) {
			return err
		}
	}

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
//	       return func(ctx context.Context, msg *AmqpMessage) error {
//	           // before
//	           err := next(ctx, msg)
//	           // after
//	           return err
//	       }
//	   }
func (mux *MessageMux) AddConsumerFeatureChain(chain ...ConsumerDecorator) *MessageMux {
	if mux.isGoroutineSafe.Load() {
		mux.mu.Lock()
		defer mux.mu.Unlock()
	}

	mux.consumerChain.AddFeature(chain...)
	return mux
}

func (mux *MessageMux) AddConsumerErrorChain(chain ...ConsumerDecorator) *MessageMux {
	if mux.isGoroutineSafe.Load() {
		mux.mu.Lock()
		defer mux.mu.Unlock()
	}

	mux.consumerChain.AddError(chain...)
	return mux
}

// RegisterConsumerFunc is suitable for use when the RoutingKey and BindingKey are an exact match.
func (mux *MessageMux) RegisterConsumerFunc(bindingKey string, fn ConsumerFunc) {
	if mux.isGoroutineSafe.Load() {
		mux.mu.Lock()
		defer mux.mu.Unlock()
	}
	mux.registerConsumerFunc(bindingKey, fn)
}

func (mux *MessageMux) registerConsumerFunc(bindingKey string, fn ConsumerFunc) {
	if isTopicKey(bindingKey) {
		panic("not allow wildcard symbol")
	}

	_, ok := mux.cNormalHandlers[bindingKey]
	if ok {
		panic("duplicate key: " + bindingKey)
	}

	mux.cNormalHandlers[bindingKey] = fn
}

func (mux *MessageMux) RemoveConsumerFunc(key string) {
	if mux.isGoroutineSafe.Load() {
		mux.mu.Lock()
		defer mux.mu.Unlock()
	}

	delete(mux.cNormalHandlers, key)
}

// RegisterConsumerFuncByTopic
// If the bindingKey contains a wildcard string * or #, use this function to return the index for remove handler.
func (mux *MessageMux) RegisterConsumerFuncByFanout(bindingKey string, fn ConsumerFunc) (index int) {
	if mux.isGoroutineSafe.Load() {
		mux.mu.Lock()
		defer mux.mu.Unlock()
	}

	if bindingKey == "" {
		n := len(mux.cBlankKeyFanoutHandlers)
		mux.cBlankKeyFanoutHandlers = append(mux.cBlankKeyFanoutHandlers, fn)
		return n
	}

	_, ok := mux.cFanoutHandlers[bindingKey]
	if ok {
		panic("duplicate key: " + bindingKey)
	}

	mux.cFanoutHandlers[bindingKey] = fn
	return -1
}

// RemoveConsumerFuncByFanout
// The index corresponds to the return value of RegisterConsumerFuncByFanout.
func (mux *MessageMux) RemoveConsumerFuncByFanout(bindingKey string, index ...int) {
	if mux.isGoroutineSafe.Load() {
		mux.mu.Lock()
		defer mux.mu.Unlock()
	}

	if bindingKey == "" {
		idx := index[0]
		mux.cBlankKeyFanoutHandlers = append(mux.cBlankKeyFanoutHandlers[:idx], mux.cBlankKeyFanoutHandlers[idx+1:]...)
		return
	}
	delete(mux.cFanoutHandlers, bindingKey)
}

func (mux *MessageMux) RegisterConsumerFuncByTopic(bindingKey string, fn ConsumerFunc) (index int) {
	if mux.isGoroutineSafe.Load() {
		mux.mu.Lock()
		defer mux.mu.Unlock()
	}

	if isTopicKey(bindingKey) {
		n := len(mux.cWildcardTopicHandlers)
		mux.cWildcardTopicHandlers = append(mux.cWildcardTopicHandlers, fn)
		return n
	}

	mux.registerConsumerFunc(bindingKey, fn)
	return -1
}

// RemoveConsumerFuncByTopic
// The index corresponds to the return value of RegisterConsumerFuncByTopic.
func (mux *MessageMux) RemoveConsumerFuncByTopic(bindingKey string, index ...int) {
	if mux.isGoroutineSafe.Load() {
		mux.mu.Lock()
		defer mux.mu.Unlock()
	}

	if isTopicKey(bindingKey) {
		idx := index[0]
		mux.cWildcardTopicHandlers = append(mux.cWildcardTopicHandlers[:idx], mux.cWildcardTopicHandlers[idx+1:]...)
		return
	}

	mux.RemoveConsumerFunc(bindingKey)
}

func isTopicKey(bindingKey string) bool {
	return strings.Contains(bindingKey, "*") || strings.Contains(bindingKey, "#")
}

func (mux *MessageMux) EnableGoroutineSafe() {
	mux.isGoroutineSafe.Store(true)
}

func (mux *MessageMux) DisableGoroutineSafe() {
	mux.isGoroutineSafe.Store(false)
}

//

func handleDefaultConsumerError(next ConsumerFunc) ConsumerFunc {
	return func(ctx context.Context, msg *AmqpMessage) error {
		err := next(ctx, msg)
		if err != nil {
			defaultLogger.Info("default error handle: consumer=%q: key=%q: payload=%q: %v",
				msg.ConsumerTag,
				msg.RoutingKey,
				msg.Body,
				err,
			)
			return err
		}
		return nil
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

func isNextFuncStopped(ctx context.Context) bool {
	isStopped := ctx.Value(nextChecker{}).(*bool)
	return *isStopped
}

// StopNextFunc
// Due to the flexible RoutingKey mechanism in rabbitmq,
// it's necessary for handlers to check whether a message belongs to them.
//
// Use Chain of Responsibility Design Pattern.
func StopNextFunc(ctx context.Context) {
	isStopped := ctx.Value(nextChecker{}).(*bool)
	*isStopped = true
	return
}
