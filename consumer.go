package rabbitHalo

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

func newAmqpConsumer(ch *Channel, useParam []UseAmqpConsumerParam) (AmqpConsumer, AmqpConsumeParam, error) {
	var param AmqpConsumeParam
	for _, replace := range useParam {
		replace(&param)
	}

	amqpConsumer, err := ch.amqpCh.Consume(
		param.QueueName,
		param.ConsumerName,
		param.AutoAck,
		param.Exclusive,
		param.NoLocal,
		param.NoWait,
		param.Args,
	)
	if err != nil {
		return nil, AmqpConsumeParam{}, fmt.Errorf("new amqp consumer=%q: %w", param.ConsumerName, err)
	}

	err = ch.amqpCh.Qos(param.QosPrefetchCount, param.QosPrefetchSize, param.QosGlobal)
	if err != nil {
		return nil, AmqpConsumeParam{}, fmt.Errorf("setup Qos: %v", err)
	}

	return amqpConsumer, param, nil
}

func newConsumer(ch *Channel, fn ConsumerFunc, useParam ...UseAmqpConsumerParam) (*Consumer, error) {
	amqpConsumer, param, err1 := newAmqpConsumer(ch, useParam)
	if err1 != nil {
		return nil, err1
	}

	consumer := &Consumer{
		Id:           param.ConsumerName,
		parent:       ch,
		queueName:    param.QueueName,
		messageFunc:  fn,
		AmqpConsumer: amqpConsumer,
	}

	consumer.unlimitedRetryAmqpConsumer = func(ctx context.Context) {
		var unlimited time.Duration = 0
		retry(unlimited, "retryAmqpConsumer: "+consumer.Id, func() error {
			select {
			case <-ctx.Done():
				return nil
			default:
				// 如果 exchange, queue 是永久存在的,
				// 不需要 setRebuilder, 可以不使用 TopologyRebuilder
				rebuilder := consumer.rebuilder
				if rebuilder != nil {
					err := rebuilder.rebuildAction(consumer.parent)
					if err != nil {
						return err
					}
				}

				consumer.parent.mu.Lock()
				defer consumer.parent.mu.Unlock()

				if consumer.isClosed.Load() {
					return nil
				}

				amqpConsume, _, err := newAmqpConsumer(consumer.parent, useParam)
				if err != nil {
					return err
				}
				consumer.AmqpConsumer = amqpConsume
			}
			return nil
		})
	}

	return consumer, nil
}

type Consumer struct {
	AmqpConsumer               AmqpConsumer
	unlimitedRetryAmqpConsumer func(ctx context.Context)
	rebuilder                  *TopologyRebuilder
	isClosed                   atomic.Bool

	Id          string
	queueName   string
	parent      *Channel
	messageFunc ConsumerFunc

	mqClose      context.Context
	commandClose func()
}

func (c *Consumer) setRebuilder(b *TopologyRebuilder) {
	c.rebuilder = b
}

func (c *Consumer) SyncServe() {
	c.SyncServeWithContext(context.Background())
}

func (c *Consumer) SyncServeWithContext(ctx context.Context) {
	withCancel, cancelFunc := context.WithCancel(ctx)
	c.mqClose = withCancel
	c.commandClose = cancelFunc

	for {
		defaultLogger.Info("consumer=%q serve start", c.Id)
	Consume:
		for {
			select {
			case <-c.mqClose.Done():
				c.commandClose()
				return
			default:
			}

			select {
			case <-c.mqClose.Done():
				c.commandClose()
				return

			case d, ok := <-c.AmqpConsumer:
				if !ok {
					defaultLogger.Debug("consumer=%q close", c.Id)
					break Consume
				}
				msg := &d
				defaultLogger.Info("mq consume=%q key=%q msg_id=%v msg=%q", c.Id, msg.RoutingKey, msg.Headers["msg_id"], string(msg.Body))
				c.messageFunc(c.mqClose, msg)
			}
		}
		c.unlimitedRetryAmqpConsumer(c.mqClose)
	}
}

func (c *Consumer) CloseConsumer() error {
	if c.isClosed.Load() {
		return nil
	}
	c.isClosed.Store(true)
	c.commandClose()
	defaultLogger.Info("consumer=%q close success", c.Id)

	c.parent.mu.Lock()
	defer c.parent.mu.Unlock()

	if err := c.parent.amqpCh.Cancel(c.Id, true); err != nil {
		return fmt.Errorf("cancel consumer=%q: %v", c.Id, err)
	}
	return nil
}

func AsyncServeConsumerAllWithContext(all []*Consumer, ctx context.Context) {
	for _, c := range all {
		consumer := c
		go func() {
			consumer.SyncServeWithContext(ctx)
		}()
	}
}

func AsyncServeConsumerAll(all []*Consumer) {
	AsyncServeConsumerAllWithContext(all, context.Background())
}

func AsyncCloseConsumerAll(all []*Consumer) {
	for _, c := range all {
		consumer := c
		go func() {
			consumer.CloseConsumer()
		}()
	}
}
