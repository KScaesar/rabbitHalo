package rabbitHalo

type AmqpKey interface {
	RoutingKey() string
	BindingKey() string
}

// ToolRoutingKey
// 使用場景: 向 mux 註冊 handler 的時候,
// 不能決定 routing key 會是什麼形式,
// 可以用此變數當成替代名稱.
const ToolRoutingKey = "ToolKey-1qaz2wsx"

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
