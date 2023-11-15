package rabbitHalo

type AmqpKey interface {
	RoutingKey() string
	BindingKey() string
	KeyKind() string
}

// DynamicRoutingKey
// 使用場景:
// 無法在伺服器啟動當下決定的 key, 後續使用 MuxStopNext 在 handler 判斷是否滿足 key 路由
//
// 事實上, DynamicRoutingKey 都可以透過自己滿足 AmqpKey 來完成路由配對,
// DynamicRoutingKey 主要是提供一個簡單的做法.
const DynamicRoutingKey = "1qaz2wsx-DynamicKey-0p;/9ol."

//

func NewKeyTopic(routingKey string, bindingKey string) TopicKey {
	return TopicKey{routingKey: routingKey, bindingKey: bindingKey}
}

type TopicKey struct {
	routingKey string
	bindingKey string
}

func (t TopicKey) RoutingKey() string {
	return t.routingKey
}

func (t TopicKey) BindingKey() string {
	return t.bindingKey
}

func (t TopicKey) KeyKind() string {
	return "topic"
}

//

func NewKeyDirect(key string) DirectKey {
	return DirectKey{key: key}
}

type DirectKey struct {
	key string
}

func (d DirectKey) RoutingKey() string {
	return d.key
}

func (d DirectKey) BindingKey() string {
	return d.key
}

func (d DirectKey) KeyKind() string {
	return "direct"
}

//

func NewKeyFanout(key string) FanoutKey {
	return FanoutKey{key: key}
}

type FanoutKey struct {
	key string
}

func (f FanoutKey) RoutingKey() string {
	return f.key
}

func (f FanoutKey) BindingKey() string {
	return ""
}

func (f FanoutKey) KeyKind() string {
	return "fanout"
}
