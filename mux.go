package rabbitHalo

import (
	"context"
	"log"
	"os"
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

	// 由於 rabbitmq 有非常靈活的 RoutingKey 機制
	// RoutingKey 和 BindingKey 可能不是精確配對
	//
	// 但此 mux 只能做到 "字串精確" 對比
	// 必須靠 handler func 自行比對 key
	// 利用 Chain of Responsibility Pattern, 找尋合適的 handler
	//
	// 以下 RoutingKey , 找尋效率差
	// 可以考慮不使用 mux 註冊 handler func
	//
	// 1. topic 類型, 也就是 wildcard *.*.*
	// 2. fanout 類型, 因為 RoutingKey 任意格式在 rabbitmq 都可以配對
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

	key := msg.RoutingKey
	handler, ok := mux.cNormalHandlers[key]
	if ok {
		return mux.consumerChain.Link(handler)(ctx, msg)
	}

	if key == "" {
		// Chain of Responsibility Pattern
		for _, fanoutHandler := range mux.cBlankKeyFanoutHandlers {
			err := mux.consumerChain.Link(fanoutHandler)(ctx, msg)
			if err != nil {
				return err
			}
			if isClaimedTask(ctx) {
				return nil
			}
		}
	}

	if fanoutHandler, exist := mux.cFanoutHandlers[key]; exist {
		return mux.consumerChain.Link(fanoutHandler)(ctx, msg)
	}

	// Chain of Responsibility Pattern
	for _, topicHandler := range mux.cWildcardTopicHandlers {
		err := mux.consumerChain.Link(topicHandler)(ctx, msg)
		if err != nil {
			return err
		}
		if isClaimedTask(ctx) {
			return nil
		}
	}

	errHandler := func(_ context.Context, _ *AmqpMessage) error { return ErrNotMatchRoutingKey }
	return mux.consumerChain.LinkError(errHandler)(ctx, msg)
}

// AddConsumerFeatureChain 執行順序參考範例 https://go.dev/play/p/I9cK-VvKjzi
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

// RegisterConsumerFunc
// 用在字串精確對比,
// 如果有動態 key 的註冊需求, 要注意會有 gc 無法回收的問題, map 記憶體不斷成長,
// 也許 handler 直接和 amqp consume( Channel.CreateConsumer ) 綁定更好.
func (mux *MessageMux) RegisterConsumerFunc(bindingKey string, fn ConsumerFunc) {
	if mux.isGoroutineSafe.Load() {
		mux.mu.Lock()
		defer mux.mu.Unlock()
	}

	if strings.Contains(bindingKey, "*") {
		panic("not allow wildcard symbol")
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
	mux.cFanoutHandlers[bindingKey] = fn
	return -1
}

// RemoveConsumerFuncByFanout index 為 RegisterConsumerFuncByFanout return value
func (mux *MessageMux) RemoveConsumerFuncByFanout(index int, bindingKey string) {
	if mux.isGoroutineSafe.Load() {
		mux.mu.Lock()
		defer mux.mu.Unlock()
	}

	if bindingKey == "" {
		mux.cBlankKeyFanoutHandlers = append(mux.cBlankKeyFanoutHandlers[:index], mux.cBlankKeyFanoutHandlers[index+1:]...)
		return
	}
	delete(mux.cFanoutHandlers, bindingKey)
}

// RegisterConsumerFuncByTopic
// 通常是 bindingKey 包含 * wildcard 才會使用此函數,
// bindingKey 無實際作用,
// 方便閱讀程式碼知道 rabbitmq 具體用了什麼的 wildcard key.
func (mux *MessageMux) RegisterConsumerFuncByTopic(bindingKey string, fn ConsumerFunc) (index int) {
	if mux.isGoroutineSafe.Load() {
		mux.mu.Lock()
		defer mux.mu.Unlock()
	}

	n := len(mux.cWildcardTopicHandlers)
	mux.cWildcardTopicHandlers = append(mux.cWildcardTopicHandlers, fn)
	return n
}

// RemoveConsumerFuncByTopic index 為 RegisterConsumerFuncByTopic return value
func (mux *MessageMux) RemoveConsumerFuncByTopic(index int) {
	if mux.isGoroutineSafe.Load() {
		mux.mu.Lock()
		defer mux.mu.Unlock()
	}

	mux.cWildcardTopicHandlers = append(mux.cWildcardTopicHandlers[:index], mux.cWildcardTopicHandlers[index+1:]...)
}

func (mux *MessageMux) EnableGoroutineSafe() {
	mux.isGoroutineSafe.Store(true)
}

func (mux *MessageMux) DisableGoroutineSafe() {
	mux.isGoroutineSafe.Store(false)
}

//

func handleDefaultConsumerError(next ConsumerFunc) ConsumerFunc {
	logger := log.New(os.Stderr, "Error: ", log.Ltime|log.Lshortfile|log.LUTC|log.Lmsgprefix)

	return func(ctx context.Context, msg *AmqpMessage) error {
		err := next(ctx, msg)
		if err != nil {
			logger.Printf("default error handle: consumer=%q: key=%q: payload=%q: %v",
				msg.ConsumerTag,
				msg.RoutingKey,
				msg.Body,
				err,
			)
			return nil
		}
		return nil
	}
}

func handleTechnicalContext(next ConsumerFunc) ConsumerFunc {
	return func(ctx context.Context, msg *AmqpMessage) error {
		ctx1 := contextWithTaskChecker(ctx)
		// ctx2 := ctx1
		lastCtx := ctx1
		return next(lastCtx, msg)
	}
}

//

type taskChecker struct{}

func contextWithTaskChecker(ctx context.Context) context.Context {
	var isClaimed bool
	return context.WithValue(ctx, taskChecker{}, &isClaimed)
}

func isClaimedTask(ctx context.Context) bool {
	isClaimed := ctx.Value(taskChecker{}).(*bool)
	return *isClaimed
}

// ClaimTask
// 由於 rabbitmq 有非常靈活的 RoutingKey 機制,
// 必須靠 handler func 自行比對, 是否該訊息屬於自己,
// 通常只有 key kind = topic, fanout 才有機會用到此函數.
func ClaimTask(ctx context.Context) {
	isClaimed := ctx.Value(taskChecker{}).(*bool)
	*isClaimed = true
}
