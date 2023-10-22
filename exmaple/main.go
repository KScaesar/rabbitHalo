package main

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/KScaesar/rabbitHalo"
)

func main() {
	pool := rabbitHalo.NewPool(3, 5, func() (*rabbitHalo.AmqpConnection, error) {
		return rabbitHalo.NewAmqpConnection("amqp://guest:guest@localhost:5672/")
	})

	// asyncUseCase(pool, 30)
	syncUseCase(pool, 40)

	// ==pool lock==
	// reuse resource
	// get connection[id=2]: min=0 total=25 qty=[8 9 8]
	// ==pool unlock==
	// ==conn[id=2] lock==
	// reuse resource
	// get channel[id=2] from conn[id=2]: min=3 total=9 qty=[2 2 2 1 2]
	// ==conn unlock==
	// starting Consume (consumer tag "notify:user25-worker0")
	// starting Consume (consumer tag "notify:user25-worker1")
	// ==pool lock==
	// reuse resource
	// get connection[id=0]: min=0 total=26 qty=[8 9 9]
	// ==pool unlock==	time.Sleep(60 * time.Minute)

	// time.Sleep(30 * time.Minute)
	// time.Sleep(30 * time.Second)
	time.Sleep(5 * time.Second)

	err := pool.Close()
	if err != nil {
		panic(err)
	}

	log.Printf("end!\n")
}

func SubscribeNotificationByUser(user string, pool *rabbitHalo.ConnectionPool) {
	conn, err := pool.AcquireConnection()
	if err != nil {
		panic(err)
	}

	ttl := 10 * time.Second
	queue := "notify:" + user
	channel, err := conn.AcquireChannel()
	if err != nil {
		panic(err)
	}

	const (
		ex1     = "broadcast"
		ex1Type = "fanout"
		key1    = "broadcast.lv0.*"
	)
	_, err1 := channel.CreateQueue(rabbitHalo.SetupDefaultQueue(ex1, ex1Type, queue, key1), rabbitHalo.SetupTemporaryQueue(ttl))
	if err1 != nil {
		log.Fatalf("%v", err)
	}

	const (
		ex2     = "condition"
		ex2Type = "topic"
		key2    = "*.*.*"
	)
	_, err2 := channel.CreateQueue(rabbitHalo.SetupDefaultQueue(ex2, ex2Type, queue, key2), rabbitHalo.SetupTemporaryQueue(ttl))
	if err2 != nil {
		log.Fatalf("%v", err2)
	}

	ex3 := "single"
	ex3Type := "direct"
	key3 := user
	_, err3 := channel.CreateQueue(rabbitHalo.SetupDefaultQueue(ex3, ex3Type, queue, key3), rabbitHalo.SetupTemporaryQueue(ttl))
	if err3 != nil {
		log.Fatalf("%v", err3)
	}

	consumerName := queue + "-worker"
	consumers, err := channel.CreateConsumers(user, queue, consumerName, 2, printMessage)
	if err != nil {
		log.Fatalf("%v", err)
	}

	consumers.Run()

	go func() {
		time.Sleep(time.Duration(rand.Int31n(10)) * time.Second)
		// consumers.Stop()
		// conn.ReleaseChannel(channel)
		// pool.ReleaseConnection(conn)
	}()
}

func syncUseCase(pool *rabbitHalo.ConnectionPool, maxWorker int) {
	go func() {
		for i := 0; i < maxWorker; i++ {
			i := i
			user := "user" + strconv.Itoa(i)
			SubscribeNotificationByUser(user, pool)
		}
	}()
}

func asyncUseCase(pool *rabbitHalo.ConnectionPool, maxWorker int) {
	for i := 0; i < maxWorker; i++ {
		i := i
		go func() {
			user := "user" + strconv.Itoa(i)
			SubscribeNotificationByUser(user, pool)
		}()
	}
}

func printMessage(_ context.Context, d *amqp.Delivery) error {
	log.Printf(
		"DeliveryTag: [%v], payload: %q, key: %v",
		d.DeliveryTag,
		d.Body,
		d.RoutingKey,
	)
	d.Ack(false)
	return nil
}
