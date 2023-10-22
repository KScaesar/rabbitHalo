package rabbitHalo

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AmqpConnection = amqp.Connection
type AmqpChannel = amqp.Channel
type AmqpConsumer = <-chan amqp.Delivery
type AmqpMessage = amqp.Delivery

type AmqpExchangeDeclareParam struct {
	ExchangeName string
	ExchangeKind string
	Durable      bool
	AutoDelete   bool
	Internal     bool
	NoWait       bool
	Args         amqp.Table
}

type AmqpQueueDeclareParam struct {
	QueueName  string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

type AmqpQueueBindParam struct {
	Key          string
	ExchangeName string
	NoWait       bool
	Args         amqp.Table
}

type AmqpConsumeParam struct {
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

// queue

type UseQueueParam func(ex *AmqpExchangeDeclareParam, q *AmqpQueueDeclareParam, bind *AmqpQueueBindParam)

func SetupDefaultQueue(exchange, exchangeType, queueName, bindingKey string) UseQueueParam {
	return func(ex *AmqpExchangeDeclareParam, q *AmqpQueueDeclareParam, bind *AmqpQueueBindParam) {
		ex.ExchangeName = exchange
		ex.ExchangeKind = exchangeType
		ex.Durable = true

		q.QueueName = queueName
		q.Durable = true

		bind.Key = bindingKey
		bind.ExchangeName = exchange
	}
}

func SetupTemporaryQueue(ttl time.Duration) UseQueueParam {
	return func(_ *AmqpExchangeDeclareParam, q *AmqpQueueDeclareParam, bind *AmqpQueueBindParam) {
		if q.Args == nil {
			q.Args = make(amqp.Table)
		}
		q.Args["x-expires"] = ttl.Milliseconds()

		if bind.Args == nil {
			bind.Args = make(amqp.Table)
		}
		bind.Args["x-expires"] = ttl.Milliseconds()
	}
}

// consumer

type UseConsumerParam func(param *AmqpConsumeParam)

func WithAutoAck(param *AmqpConsumeParam) {
	if param.Args == nil {
		param.Args = make(amqp.Table)
	}

	param.AutoAck = true
}

// Todo: producer not implement
