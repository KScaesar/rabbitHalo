//go:build integration_test

package rabbitHalo_test

import (
	"context"
	"fmt"
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
	queue1 := "group:" + user
	queue2 := "private:" + user

	channel, err := conn.AcquireChannel()
	if err != nil {
		panic(err)
	}

	// queue

	const (
		ex1     = "broadcast"
		ex1Type = "fanout"
		key1    = "fanout.bindKey.invalid"
	)
	mux.RegisterConsumerFuncByFanout("", printMessage)
	// mux.RegisterConsumerFuncByFanout(key1, printMessage)
	_, err1 := channel.CreateQueue(rabbitHalo.SetupDefaultQueue(ex1, ex1Type, queue1, key1), rabbitHalo.SetupTemporaryQueue(ttl))
	if err1 != nil {
		rabbitHalo.DefaultLogger().Fatal("%v", err)
	}

	const (
		ex2     = "condition"
		ex2Type = "topic"
		key2    = "notify.*.*.*"
	)
	mux.RegisterConsumerFuncByTopic("notify.*.*.*", topicPrintMessage)
	_, err2 := channel.CreateQueue(rabbitHalo.SetupDefaultQueue(ex2, ex2Type, queue1, key2), rabbitHalo.SetupTemporaryQueue(ttl))
	if err2 != nil {
		rabbitHalo.DefaultLogger().Fatal("%v", err2)
	}

	ex3 := "single"
	ex3Type := "direct"
	key3 := user
	mux.RegisterConsumerFunc(key3, printMessage)
	_, err3 := channel.CreateQueue(rabbitHalo.SetupDefaultQueue(ex3, ex3Type, queue2, key3), rabbitHalo.SetupTemporaryQueue(ttl))
	if err3 != nil {
		rabbitHalo.DefaultLogger().Fatal("%v", err3)
	}

	// consumer

	consumerName1 := fmt.Sprintf("%v-worker", queue1)
	consumer1, err := channel.CreateConsumers(queue1, consumerName1, mux.ServeConsume, 2, rabbitHalo.SetupDefaultQos)
	if err != nil {
		rabbitHalo.DefaultLogger().Fatal("%v", err)
	}
	consumerName2 := fmt.Sprintf("%v-worker", queue2)
	consumer2, err := channel.CreateConsumer(queue2, consumerName2, mux.ServeConsume)
	if err != nil {
		rabbitHalo.DefaultLogger().Fatal("%v", err)
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

	// closeTime := 10 * time.Second
	closeTime := 10 * time.Minute

	syncUseCase(connection, mux, 3)
	// asyncUseCase(connection, mux, 30)

	time.Sleep(closeTime)
}

func topicPrintMessage(ctx context.Context, d *rabbitHalo.AmqpMessage) error {
	if d.RoutingKey != "notify.*.*.ios" {
		return nil
	}
	rabbitHalo.StopNextFunc(ctx)

	rabbitHalo.DefaultLogger().Info(
		"cId=%q: DeliveryTag=[%v]: payload=%q: key=%v",
		d.ConsumerTag,
		d.DeliveryTag,
		d.Body,
		d.RoutingKey,
	)
	d.Ack(false)
	return nil
}

func printMessage(ctx context.Context, d *rabbitHalo.AmqpMessage) error {
	// if d.RoutingKey != "fanout.bindKey.invalid" {
	// 	return nil
	// }
	// rabbitHalo.StopNextFunc(ctx)

	rabbitHalo.DefaultLogger().Info(
		"cId=%q: DeliveryTag=[%v]: payload=%q: key=%v",
		d.ConsumerTag,
		d.DeliveryTag,
		d.Body,
		d.RoutingKey,
	)
	d.Ack(false)
	return nil
}
