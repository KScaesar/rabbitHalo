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

	// With a prefetch count greater than zero, the server will deliver that many
	// messages to consumers before acknowledgments are received.  The server ignores
	// this option when consumers are started with noAck because no acknowledgments
	// are expected or sent.
	//
	// In order to defeat that we can set the prefetch count with the value of 1.
	// This tells RabbitMQ not to give more than one message to a worker at a time.
	// Or, in other words, don't dispatch a new message to a worker until it has
	// processed and acknowledged the previous one.
	// Instead, it will dispatch it to the next worker that is not still busy.
	QosPrefetchCount int

	// With a prefetch size greater than zero, the server will try to keep at least
	// that many bytes of deliveries flushed to the network before receiving
	// acknowledgments from the consumers.  This option is ignored when consumers are
	// started with noAck.
	QosPrefetchSize int

	// When global is true, these Qos settings apply to all existing and future
	// consumers on all channels on the same connection.  When false, the Channel.Qos
	// settings will apply to all existing and future consumers on this channel.
	//
	// Please see the RabbitMQ Consumer Prefetch documentation for an explanation of
	// how the global flag is implemented in RabbitMQ, as it differs from the
	// AMQP 0.9.1 specification in that global Qos settings are limited in scope to
	// channels, not connections (https://www.rabbitmq.com/consumer-prefetch.html).
	QosGlobal bool
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

func SetupDeadLetterQueue(exchange, exchangeType, queueName, bindingKey string) UseQueueParam {
	return func(ex *AmqpExchangeDeclareParam, q *AmqpQueueDeclareParam, bind *AmqpQueueBindParam) {
		ex.ExchangeName = exchange
		ex.ExchangeKind = exchangeType
		ex.Durable = true

		q.QueueName = queueName
		q.Durable = true
		if q.Args == nil {
			q.Args = make(amqp.Table)
		}
		q.Args["x-dead-letter-exchange"] = exchange

		bind.Key = bindingKey
		bind.ExchangeName = exchange
	}
}

// consumer

type UseConsumerParam func(param *AmqpConsumeParam)

func SetupAutoAck(param *AmqpConsumeParam) {
	if param.Args == nil {
		param.Args = make(amqp.Table)
	}

	param.AutoAck = true
}

func SetupDefaultQos(param *AmqpConsumeParam) {
	param.QosPrefetchCount = 1
}

// Todo: producer not implement
