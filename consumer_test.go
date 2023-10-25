//go:build integration_test

package rabbitHalo_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/KScaesar/rabbitHalo"
)

func syncUseCase(conn *rabbitHalo.Connection, mux *rabbitHalo.MessageMux, maxWorker int) {
	go func() {
		for i := 0; i < maxWorker; i++ {
			user := "user" + strconv.Itoa(i)
			SubscribeNotificationByUser(user, conn, mux)
		}
	}()
}

func asyncUseCase(conn *rabbitHalo.Connection, mux *rabbitHalo.MessageMux, maxWorker int) {
	for i := 0; i < maxWorker; i++ {
		i := i
		go func() {
			user := "user" + strconv.Itoa(i)
			// time.Sleep(time.Duration(rand.Int31n(5)) * time.Second)
			SubscribeNotificationByUser(user, conn, mux)
		}()
	}
}

func SubscribeNotificationByUser(user string, conn *rabbitHalo.Connection, mux *rabbitHalo.MessageMux) {
	ttl := 5 * time.Second
	queue1 := "notify:" + user
	queue2 := "private:" + user

	channel, err := conn.AcquireChannel()
	if err != nil {
		panic(err)
	}

	const (
		ex1     = "broadcast"
		ex1Type = "fanout"
		key1    = "fanout.bindKey.invalid"
	)
	_, err1 := channel.CreateQueue(rabbitHalo.SetupDefaultQueue(ex1, ex1Type, queue1, key1), rabbitHalo.SetupTemporaryQueue(ttl))
	if err1 != nil {
		log.Fatalf("%v", err)
	}

	const (
		ex2     = "condition"
		ex2Type = "topic"
		key2    = "notify.*.*.*"
	)
	_, err2 := channel.CreateQueue(rabbitHalo.SetupDefaultQueue(ex2, ex2Type, queue1, key2), rabbitHalo.SetupTemporaryQueue(ttl))
	if err2 != nil {
		log.Fatalf("%v", err2)
	}

	ex3 := "single"
	ex3Type := "direct"
	key3 := user
	_, err3 := channel.CreateQueue(rabbitHalo.SetupDefaultQueue(ex3, ex3Type, queue2, key3), rabbitHalo.SetupTemporaryQueue(ttl))
	if err3 != nil {
		log.Fatalf("%v", err3)
	}

	//

	consumerName1 := fmt.Sprintf("%v-worker", queue1)
	consumer1, err := channel.CreateConsumers(queue1, consumerName1, 2, mux.ServeConsume, rabbitHalo.SetupDefaultQos)
	if err != nil {
		log.Fatalf("%v", err)
	}

	consumerName2 := fmt.Sprintf("%v-worker", queue2)
	consumer2, err := channel.CreateConsumer(queue2, consumerName2, mux.ServeConsume)
	if err != nil {
		log.Fatalf("%v", err)
	}

	var consumers rabbitHalo.ConsumerAll
	consumers.
		AddConsumer(consumer1...).
		AddConsumer(consumer2).
		AsyncRun()

	go func() {
		time.Sleep(time.Duration(rand.Int31n(10)) * time.Second)
		// consumers.Stop()
		// mux.RemoveConsumerFunc(user)
	}()
}

func TestConsumer_Consume(t *testing.T) {
	pool := rabbitHalo.NewPool("amqp://guest:guest@127.0.0.1:5672", 3, 5)
	connection, err := pool.AcquireConnection()
	require.NoError(t, err)

	mux := rabbitHalo.NewMessageMux()
	mux.AddConsumerFeatureChain(testMiddleware1, testMiddleware2)
	mux.RegisterConsumerFuncByFanout("", printMessage)
	mux.RegisterConsumerFuncByFanout("fanout.bindKey.invalid", fanoutPrintMessage)
	mux.RegisterConsumerFuncByFanout("error", raiseError)
	mux.RegisterConsumerFuncByTopic("notify.*.*.*", topicPrintMessage)

	// closeTime := 10 * time.Second
	closeTime := 10 * time.Minute

	// syncUseCase(connection, mux, 30)
	asyncUseCase(connection, mux, 30)

	time.Sleep(closeTime)
}

func topicPrintMessage(ctx context.Context, d *rabbitHalo.AmqpMessage) error {
	if d.RoutingKey != "notify.*.*.ios" {
		return nil
	}
	rabbitHalo.ClaimTask(ctx)

	log.Printf(
		"Topic DeliveryTag: [%v], payload: %q, key: %v",
		d.DeliveryTag,
		d.Body,
		d.RoutingKey,
	)
	d.Ack(false)
	return nil
}

func printMessage(ctx context.Context, d *rabbitHalo.AmqpMessage) error {
	log.Printf(
		"DeliveryTag: [%v], payload: %q, key: %v",
		d.DeliveryTag,
		d.Body,
		d.RoutingKey,
	)
	d.Ack(false)
	return nil
}

func fanoutPrintMessage(ctx context.Context, d *rabbitHalo.AmqpMessage) error {
	if d.RoutingKey != "fanout.bindKey.invalid" {
		return nil
	}
	rabbitHalo.ClaimTask(ctx)

	log.Printf(
		"Fanout DeliveryTag: [%v], payload: %q, key: %v",
		d.DeliveryTag,
		d.Body,
		d.RoutingKey,
	)
	d.Ack(false)
	return nil
}

func raiseError(ctx context.Context, d *rabbitHalo.AmqpMessage) error {
	if d.RoutingKey != "error" {
		return nil
	}
	rabbitHalo.ClaimTask(ctx)

	return errors.New("project 開天窗")
}

func testMiddleware1(next rabbitHalo.ConsumerFunc) rabbitHalo.ConsumerFunc {
	return func(ctx context.Context, msg *rabbitHalo.AmqpMessage) error {
		log.Printf("middleware before 1")
		err := next(ctx, msg)
		log.Printf("middleware after 1")
		return err
	}
}

func testMiddleware2(next rabbitHalo.ConsumerFunc) rabbitHalo.ConsumerFunc {
	return func(ctx context.Context, msg *rabbitHalo.AmqpMessage) error {
		log.Printf("middleware before 2")
		err := next(ctx, msg)
		log.Printf("middleware after 2")
		return err
	}
}
