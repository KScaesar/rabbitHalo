package rabbitHalo

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AmqpConnection = amqp.Connection
type AmqpChannel = amqp.Channel
type AmqpQueue = amqp.Queue
type AmqpConsumer = <-chan amqp.Delivery
type AmqpMessage = amqp.Delivery
type AmqpError = amqp.Error

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
	QueueName    string
	ConsumerName string
	AutoAck      bool
	Exclusive    bool
	NoLocal      bool
	NoWait       bool
	Args         amqp.Table

	QosPrefetchCount int
	QosPrefetchSize  int
	QosGlobal        bool
}

// exchange

func DefaultExchangeParam(exchangeName, exchangeKind string) *AmqpExchangeDeclareParam {
	return &AmqpExchangeDeclareParam{
		ExchangeName: exchangeName,
		ExchangeKind: exchangeKind,
		Durable:      true,
		AutoDelete:   false,
		Internal:     false,
		NoWait:       false,
		Args:         make(amqp.Table),
	}
}

// queue

type UseAmqpQueueParam func(q *AmqpQueueDeclareParam, bind *AmqpQueueBindParam)

func DefaultQueueParam(exchangeName, queueName string, key AmqpKey) UseAmqpQueueParam {
	return func(q *AmqpQueueDeclareParam, bind *AmqpQueueBindParam) {
		q.QueueName = queueName
		q.Durable = true

		bind.Key = key.BindingKey()
		bind.ExchangeName = exchangeName
	}
}

func TemporaryQueueParam(ttl time.Duration) UseAmqpQueueParam {
	return func(q *AmqpQueueDeclareParam, bind *AmqpQueueBindParam) {
		if q.Args == nil {
			q.Args = make(amqp.Table)
		}
		q.Args[amqp.QueueTTLArg] = ttl.Milliseconds()

		if bind.Args == nil {
			bind.Args = make(amqp.Table)
		}
		bind.Args[amqp.QueueTTLArg] = ttl.Milliseconds()
	}
}

func DeadLetterQueueParam(exchange, exchangeType, queueName string, key AmqpKey) (*AmqpExchangeDeclareParam, UseAmqpQueueParam) {
	exParam := &AmqpExchangeDeclareParam{
		ExchangeName: exchange,
		ExchangeKind: exchangeType,
		Durable:      true,
		AutoDelete:   false,
		Internal:     false,
		NoWait:       false,
		Args:         nil,
	}

	useQueue := func(q *AmqpQueueDeclareParam, bind *AmqpQueueBindParam) {
		q.QueueName = queueName
		q.Durable = true
		if q.Args == nil {
			q.Args = make(amqp.Table)
		}
		q.Args["x-dead-letter-exchange"] = exchange

		bind.Key = key.BindingKey()
		bind.ExchangeName = exchange
	}

	return exParam, useQueue
}

// consumer

type UseAmqpConsumerParam func(param *AmqpConsumeParam)

func DefaultConsumerParam(queueName string, consumerName string) UseAmqpConsumerParam {
	return func(param *AmqpConsumeParam) {
		param.QueueName = queueName
		param.ConsumerName = consumerName
		DefaultQos(param)
	}
}

func DefaultQos(param *AmqpConsumeParam) {
	param.QosPrefetchCount = 1
}

func AutoAck(param *AmqpConsumeParam) {
	if param.Args == nil {
		param.Args = make(amqp.Table)
	}

	param.AutoAck = true
}

// Todo: producer not implement
