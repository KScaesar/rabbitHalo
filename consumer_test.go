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

func syncUseCase(client *rabbitHalo.Client, maxWorker int) {
	go func() {
		for i := 0; i < maxWorker; i++ {
			user := "user" + strconv.Itoa(i)
			testToolSubscribeNotificationByUser(user, client)
		}
	}()
}

func asyncUseCase(client *rabbitHalo.Client, maxWorker int) {
	for i := 0; i < maxWorker; i++ {
		i := i
		go func() {
			user := "user" + strconv.Itoa(i)
			// time.Sleep(time.Duration(rand.Int31n(5)) * time.Second)
			testToolSubscribeNotificationByUser(user, client)
		}()
	}
}

func testToolSubscribeNotificationByUser(user string, client *rabbitHalo.Client) {
	mux := rabbitHalo.NewMessageMux()

	conn, err := client.AcquireConnection()
	if err != nil {
		rabbitHalo.DefaultLogger().Fatal("AcquireConnection: %v", err)
		return
	}

	channel, err := conn.AcquireChannel()
	if err != nil {
		rabbitHalo.DefaultLogger().Fatal("AcquireChannel: %v", err)
		return
	}

	ttl := 60 * time.Second
	queue1 := "group." + user
	queue2 := "private." + user

	var (
		ex1     = "broadcast"
		key1    = rabbitHalo.NewKeyFanout("fanout.bindKey.invalid")
		ex1Type = "fanout"
	)
	var (
		ex2 = "condition"
		// key2    = rabbitHalo.NewKeyTopic("notify.shop.ios.user", "notify.*.*.*")
		key2    = rabbitHalo.NewKeyTopic("notify.shop.ios.user", "")
		ex2Type = "topic"
	)
	mux.RegisterConsumerFastHook(key1, printMessageByDynamic)
	mux.RegisterConsumerSlowHook(key2, topicPrintMessage)
	consumerName1 := fmt.Sprintf("%v-worker", queue1)

	consumer1, err := rabbitHalo.NewTopologyRebuilder(channel).
		QuicklyBuildExchange(rabbitHalo.DefaultExchangeParam(ex1, ex1Type)).
		QuicklyBuildQueue(rabbitHalo.DefaultQueueParam(ex1, queue1, key1), rabbitHalo.TemporaryQueueParam(ttl)).
		QuicklyBuildExchange(rabbitHalo.DefaultExchangeParam(ex2, ex2Type)).
		QuicklyBuildQueue(rabbitHalo.DefaultQueueParam(ex2, queue1, key2), rabbitHalo.TemporaryQueueParam(ttl)).
		BuildConsumer(mux.ServeConsume, rabbitHalo.DefaultConsumerParam(queue1, consumerName1))
	if err != nil {
		rabbitHalo.DefaultLogger().Fatal("consumer1: %v", err)
		return
	}

	var (
		ex3     = "single"
		key3    = rabbitHalo.NewKeyDirect(user)
		ex3Type = "direct"
	)
	mux.RegisterConsumerFastHook(key3, printMessage)
	consumerName2 := fmt.Sprintf("%v-worker", queue2)

	consumer2, err := rabbitHalo.NewTopologyRebuilder(channel).
		QuicklyBuildExchange(rabbitHalo.DefaultExchangeParam(ex3, ex3Type)).
		QuicklyBuildQueue(rabbitHalo.DefaultQueueParam(ex3, queue2, key3), rabbitHalo.TemporaryQueueParam(ttl)).
		BuildConsumer(mux.ServeConsume, rabbitHalo.DefaultConsumerParam(queue2, consumerName2))
	if err != nil {
		rabbitHalo.DefaultLogger().Fatal("consumer2: %v", err)
		return
	}

	consumers := []*rabbitHalo.Consumer{
		consumer1,
		consumer2,
	}
	rabbitHalo.AsyncServeConsumerAll(consumers)

	go func() {
		var offset int32 = 60
		time.Sleep(time.Duration(rand.Int31n(30)+offset) * time.Second)
		rabbitHalo.AsyncCloseConsumerAll(consumers)
		channel.CloseChannel()
	}()
}

func TestConsumer_Consume(t *testing.T) {
	client, err := rabbitHalo.NewClient("amqp://guest:guest@127.0.0.1:5672", 3, 5)
	require.NoError(t, err)

	// closeTime := 10 * time.Second
	closeTime := 10 * time.Minute

	// syncUseCase(client, 2)
	asyncUseCase(client, 50)

	time.Sleep(closeTime)
	err = client.CloseClient()
	require.NoError(t, err)
}

func topicPrintMessage(ctx context.Context, d *rabbitHalo.AmqpMessage) error {
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

func printMessageByDynamic(ctx context.Context, d *rabbitHalo.AmqpMessage) error {
	if d.RoutingKey != "" {
		return nil
	}
	rabbitHalo.AckMessage(ctx)
	return printMessage(ctx, d)
}

func printMessage(ctx context.Context, d *rabbitHalo.AmqpMessage) error {
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
