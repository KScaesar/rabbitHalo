package rabbitHalo

import (
	"context"
	"fmt"
	"sync"
)

func NewMessageMux() *MessageMux {
	mux := &MessageMux{
		consumerFastHooks:      make(map[string]ConsumerFunc),
		consumerSlowHooks:      make(map[string][]ConsumerFunc),
		consumerBroadcastHooks: make(map[string][]ConsumerFunc),
		isBroadcastAutoAck:     true,
	}
	return mux
}

type MessageMux struct {
	mu                     sync.RWMutex
	consumerFastHooks      map[string]ConsumerFunc // RoutingKey:Func
	consumerSlowHooks      map[string][]ConsumerFunc
	consumerBroadcastHooks map[string][]ConsumerFunc
	isBroadcastAutoAck     bool
	consumerChain          ConsumerChain
	consumerKeys           []AmqpKey
}

func (mux *MessageMux) ServeConsume(ctx context.Context, msg *AmqpMessage) error {
	return handleTechnicalContext(mux.serveConsume)(ctx, msg)
}

func (mux *MessageMux) serveConsume(ctx context.Context, msg *AmqpMessage) error {
	mux.mu.RLock()
	defer mux.mu.RUnlock()

	routingKey := msg.RoutingKey
	targetKeys := []string{routingKey, ToolRoutingKey}

	if fn, ok := mux.consumerFastHooks[routingKey]; ok {
		return mux.consumerChain.Link(fn)(ctx, msg)
	}

	for _, key := range targetKeys {
		if fnAll, ok := mux.consumerSlowHooks[key]; ok {
			isStop := func() bool { return hadAckMessage(ctx) }
			return mux.consumerChain.LinkAll(fnAll, isStop)(ctx, msg)
		}
	}

	for _, key := range targetKeys {
		if fnAll, ok := mux.consumerBroadcastHooks[key]; ok {
			if mux.isBroadcastAutoAck {
				msg.Ack(false)
			}
			notStopWhenBroadcast := func() bool { return false }
			return mux.consumerChain.LinkAll(fnAll, notStopWhenBroadcast)(ctx, msg)
		}
	}

	errFn := func(_ context.Context, _ *AmqpMessage) error { return ErrNotFoundHandler }
	return mux.consumerChain.LinkError(errFn)(ctx, msg)
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

// RegisterConsumerFastHook 利用 map 找出唯一消費者 big O(1)
func (mux *MessageMux) RegisterConsumerFastHook(key AmqpKey, fn ConsumerFunc) {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	routingKey := key.RoutingKey()
	if routingKey == "" {
		panic("RoutingKey is empty")
	}
	if routingKey == ToolRoutingKey {
		panic("not allow ToolRoutingKey")
	}
	_, ok := mux.consumerFastHooks[routingKey]
	if ok {
		panic("duplicate key: " + routingKey)
	}

	mux.consumerFastHooks[routingKey] = fn
	mux.addKey(key)
}

func (mux *MessageMux) RemoveConsumerFastHook(key AmqpKey) {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	delete(mux.consumerFastHooks, key.RoutingKey())
}

// RegisterConsumerSlowHook
// 利用 array 找出唯一消費者 big O(N), 情境可能是 routingKey 比較複雜, 又懶得自己實現 AmqpKey interface.
// handler 需要使用 AckMessage 認領訊息, 藉此停止尋找唯一消費者.
func (mux *MessageMux) RegisterConsumerSlowHook(key AmqpKey, fn ConsumerFunc) (idx int) {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	return mux.registerConsumerHook(mux.consumerSlowHooks, key, fn)
}

func (mux *MessageMux) RemoveConsumerSlowHook(key AmqpKey, idx int) {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	mux.removeConsumerHook(mux.consumerSlowHooks, key, idx)
}

// SetBroadcastAutoAck
// amqp message 可能滿足多個 broadcast handler,
// 不可以每個 Handler 進行 amqp.ack,
// 可以選擇讓 mux 自動處理, 或是要自己撰寫額外的 handler 處理.
//
// 預設是會自動 ack
func (mux *MessageMux) SetBroadcastAutoAck(isAuto bool) {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	mux.isBroadcastAutoAck = isAuto
}

// RegisterConsumerBroadcastHook 廣播讓每個消費者都處理消息
func (mux *MessageMux) RegisterConsumerBroadcastHook(key AmqpKey, fn ConsumerFunc) (idx int) {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	return mux.registerConsumerHook(mux.consumerBroadcastHooks, key, fn)
}

func (mux *MessageMux) RemoveConsumerBroadcastHook(key AmqpKey, idx int) {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	mux.removeConsumerHook(mux.consumerBroadcastHooks, key, idx)
}

func (mux *MessageMux) registerConsumerHook(source map[string][]ConsumerFunc, key AmqpKey, fn ConsumerFunc) (index int) {
	routingKey := key.RoutingKey()
	if routingKey == "" {
		panic("RoutingKey is empty")
	}

	fnAll := source[routingKey]
	n := len(fnAll)
	source[routingKey] = append(fnAll, fn)
	mux.addKey(key)
	return n
}

// removeConsumerHook
// The idx corresponds to the return value of registerConsumerHook
func (mux *MessageMux) removeConsumerHook(source map[string][]ConsumerFunc, key AmqpKey, idx int) {
	routingKey := key.RoutingKey()
	fnAll, exist := source[routingKey]
	if !exist {
		return
	}

	n := len(fnAll)
	if idx >= n {
		return
	}
	source[routingKey] = append(source[routingKey][:n], source[routingKey][n+1:]...)
}

func (mux *MessageMux) addKey(key AmqpKey) {
	mux.consumerKeys = append(mux.consumerKeys, key)
}

// PrintKeys 開發檢查用途, 從整體看看 key 的關聯性
func (mux *MessageMux) PrintKeys(name string) {
	fmt.Printf("=== %v ConsumerKeys: ===\n", name)
	for _, key := range mux.consumerKeys {
		fmt.Printf("  RoutingKey=%q, BindingKey=%q  \n", key.RoutingKey(), key.BindingKey())
	}
	fmt.Println()
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

			defaultLogger.
				WithMessageId(GetMessageId(msg)).
				Error(
					"default error handle: msgType=%q: consumer=%q: key=%q: payload=%q: %v",
					msg.Type, msg.ConsumerTag, msg.RoutingKey, msg.Body, err,
				)
			return err
		}
	}
}

func handleTechnicalContext(next ConsumerFunc) ConsumerFunc {
	return func(ctx context.Context, msg *AmqpMessage) error {
		ctx1 := contextWithAckChecker(ctx)
		// ctx2 := ctx1
		lastCtx := ctx1
		return next(lastCtx, msg)
	}
}

//

type ackChecker struct{}

func contextWithAckChecker(ctx context.Context) context.Context {
	var hadAck bool
	return context.WithValue(ctx, ackChecker{}, &hadAck)
}

func hadAckMessage(ctx context.Context) bool {
	hadAck := ctx.Value(ackChecker{}).(*bool)
	return *hadAck
}

// AckMessage 應用層方面的 ack, 不要和基礎設施層的 amqp.ack 混在一起討論.
// 有使用 mux 註冊 handlerFunc, 才會使用到 AckMessage, 目的是為了避免執行不必要的消費者.
func AckMessage(ctx context.Context) {
	hadAck := ctx.Value(ackChecker{}).(*bool)
	*hadAck = true
}
