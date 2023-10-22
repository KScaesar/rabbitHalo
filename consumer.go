package rabbitHalo

import (
	"context"
	"fmt"
	"log"
	"sync"
)

type ConsumerHandlerFunc func(context.Context, *AmqpMessage) error

type Consumer struct {
	Name           string
	messageHandler ConsumerHandlerFunc
	amqpConsumer   AmqpConsumer
	done           chan struct{}

	Parent    *Channel
	queueName string

	// consumer belong to who
	Owner string
}

func (c *Consumer) RunConsume() {
	ctx := context.Background()
	for d := range c.amqpConsumer {
		msg := &d
		c.messageHandler(ctx, msg)
	}
	close(c.done)
}

func (c *Consumer) Shutdown() error {
	if err := c.Parent.Cancel(c.Name, false); err != nil {
		return fmt.Errorf("concumer=%v: cancel: %v", c.Name, err)
	}
	<-c.done
	log.Printf("consumer Shutdown: %v", c.Name)

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

type ConsumerAll []Consumer

func (all ConsumerAll) Run() {
	for _, consumer := range all {
		consumer := consumer
		go func() {
			consumer.RunConsume()
		}()
	}
}

func (all ConsumerAll) Stop() {
	wg := sync.WaitGroup{}
	for _, consumer := range all {
		consumer := consumer
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := consumer.Shutdown()
			if err != nil {
				log.Printf("consumer=%v: Shutdown: %v", consumer.Name, err)
			}
		}()
	}
	wg.Wait()
	return
}
