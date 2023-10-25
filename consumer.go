package rabbitHalo

import (
	"context"
	"fmt"
	"log"
	"sync"
)

func newConsumer(queueName string, consumerId string, ch *Channel, fn ConsumerFunc, useParam ...UseConsumerParam) (*Consumer, error) {
	var param AmqpConsumeParam
	for _, replace := range useParam {
		replace(&param)
	}

	// log.Printf("starting Consume (consumer tag %q)", cTag)
	amqpConsumer, err := ch.Consume(
		queueName,
		consumerId,
		param.AutoAck,
		param.Exclusive,
		param.NoLocal,
		param.NoWait,
		param.Args,
	)
	if err != nil {
		return nil, fmt.Errorf("amqp comsum: %v", err)
	}

	consumer := &Consumer{
		Parent:         ch,
		queueName:      queueName,
		messageHandler: fn,
		amqpConsumer:   amqpConsumer,
		Id:             consumerId,
		done:           make(chan struct{}),
	}
	return consumer, nil
}

type Consumer struct {
	Id             string
	messageHandler ConsumerFunc
	amqpConsumer   AmqpConsumer
	done           chan struct{}

	Parent    *Channel
	queueName string
}

func (c *Consumer) SyncServe() {
	c.SyncServeWithContext(context.Background())
}

func (c *Consumer) SyncServeWithContext(ctx context.Context) {
	for d := range c.amqpConsumer {
		msg := &d
		c.messageHandler(ctx, msg)
	}
	close(c.done)
}

func (c *Consumer) Shutdown() error {
	if err := c.Parent.Cancel(c.Id, false); err != nil {
		return fmt.Errorf("concumer=%v: cancel: %v", c.Id, err)
	}
	<-c.done
	log.Printf("consumer Shutdown: %v", c.Id)

	// queue, err := c.Parent.QueueInspect(c.queueName)
	// if err != nil {
	// 	return fmt.Errorf("queue=%v: view : %w", c.queueName, err)
	// }
	//
	// log.Printf("queue=%v: consumer qty: %v", c.queueName, queue.Consumers)
	// if queue.Consumers == 0 {
	// 	err := c.Parent.Close()
	// 	if err != nil {
	// 		return fmt.Errorf("close Parent: %v", err)
	// 	}
	// 	log.Printf("Parent close!")
	// }

	return nil
}

type ConsumerAll []*Consumer

func (all *ConsumerAll) AsyncRun() {
	for _, consumer := range *all {
		consumer := consumer
		go func() {
			consumer.SyncServe()
		}()
	}
}

func (all *ConsumerAll) Stop() {
	wg := sync.WaitGroup{}
	for _, consumer := range *all {
		consumer := consumer
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := consumer.Shutdown()
			if err != nil {
				log.Printf("consumer=%v: Shutdown: %v", consumer.Id, err)
			}
		}()
	}
	wg.Wait()
	return
}

func (all *ConsumerAll) AddConsumer(cAll ...*Consumer) *ConsumerAll {
	*all = append(*all, cAll...)
	return all
}
