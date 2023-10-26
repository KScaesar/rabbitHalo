package rabbitHalo

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func newAmqpConnect(amqpUri string) (conn *AmqpConnection, Err error) {
	retryMaxTime := time.Minute
	// retryMaxTime := 10 * time.Second

	cnt := 0
	Err = retry(retryMaxTime, func() (err error) {
		cnt++
		defaultLogger.Info("dail rabbitmq %v times", cnt)

		conn, err = amqp.Dial(amqpUri)
		if err != nil {
			return err
		}
		return nil
	})

	go func() {
		err := <-conn.NotifyClose(make(chan *amqp.Error))
		defaultLogger.Info("close connection: %v", err)
	}()

	return
}

// pool

// NewPool pool 有 connectionMaxQty 個 connection , 每個 conn 有 channelMaxQty 個 channel,
// 因此總數量是 connectionMaxQty * channelMaxQty
func NewPool(amqpUri string, connectionMaxQty, channelMaxQty int) *ConnectionPool {
	pool := &ConnectionPool{
		connectionAll: make([]*Connection, connectionMaxQty),
		channelMaxQty: channelMaxQty,
		strategy:      NewMinUsageRateStrategy(connectionMaxQty),
		factory: func() (*amqp.Connection, error) {
			return newAmqpConnect(amqpUri)
		},
	}
	pool.strategy.InitChildStrategy(connectionMaxQty)
	return pool
}

type ConnectionPool struct {
	mu            sync.Mutex
	connectionAll []*Connection
	channelMaxQty int
	strategy      *MinUsageRateStrategy
	factory       func() (*amqp.Connection, error)
	done          atomic.Bool
}

func (p *ConnectionPool) AcquireConnection() (conn *Connection, err error) {
	// if p.done.Load() {
	// 	return nil, errors.New("pool had been closed")
	// }

	p.mu.Lock()
	defaultLogger.Info("==pool lock==")
	defer func() {
		minIndex, totalScore, listQty := p.strategy.ViewUsageQty()
		defaultLogger.Info("get connection[id=%v]: min=%v totalScore=%v qty=%v\n", conn.Id, minIndex, totalScore, listQty)
		defaultLogger.Info("==pool unlock==")
		p.mu.Unlock()
	}()

	return lazyNewResource(p.strategy, p.connectionAll, func(id int) (conn *Connection, err error) {
		amqpConn, err := p.factory()
		if err != nil {
			return nil, err
		}
		connection := newConnection(id, amqpConn, p, p.channelMaxQty)
		p.strategy.SetChildStrategy(id, connection.strategy)
		return connection, nil
	})
}

func newConnection(id int, amqpConn *AmqpConnection, pool *ConnectionPool, channelMaxQty int) *Connection {
	connection := &Connection{
		Id:             id,
		channelAll:     make([]*Channel, channelMaxQty),
		strategy:       NewMinUsageRateStrategy(channelMaxQty),
		Parent:         pool,
		AmqpConnection: amqpConn,
	}
	return connection
}

func (p *ConnectionPool) ReleaseConnection(conn *Connection) {
	// if p.done.Load() {
	// 	return
	// }

	p.mu.Lock()
	p.strategy.UpdateByRelease(conn.Id)
	p.mu.Unlock()
}

func (p *ConnectionPool) Close() error {
	// if p.done.Swap(true) {
	// 	return errors.New("pool had been closed")
	// }

	for _, connection := range p.connectionAll {
		err := connection.Close()
		if err != nil {
			return fmt.Errorf("AMQP connection close error: %w", err)
		}
	}

	return nil
}

// connection

type Connection struct {
	mu         sync.Mutex
	Id         int
	channelAll []*Channel
	strategy   *MinUsageRateStrategy
	Parent     *ConnectionPool
	*AmqpConnection
}

func (c *Connection) AcquireChannel() (ch *Channel, err error) {
	c.mu.Lock()
	// defaultLogger.Info("==conn[id=%v] lock==", c.Id)
	defer func() {
		minIndex, totalScore, listQty := c.strategy.ViewUsageQty()
		defaultLogger.Info("get channel[id=%v] from conn[id=%v]: min=%v totalScore=%v qty=%v\n", ch.Id, c.Id, minIndex, totalScore, listQty)
		// defaultLogger.Info("get channel[id=%v]: min=%v totalScore=%v qty=%v\n", ch.Id, minIndex, totalScore, listQty)
		// defaultLogger.Info("get channel from conn[id=%v]: min=%v totalScore=%v qty=%v\n", c.Id, minIndex, totalScore, listQty)
		// defaultLogger.Info("==conn unlock==")
		c.mu.Unlock()
	}()

	return lazyNewResource(c.strategy, c.channelAll, func(id int) (*Channel, error) {
		channel, err := c.AmqpConnection.Channel()
		if err != nil {
			return nil, err
		}
		return &Channel{
			Id:          id,
			Parent:      c,
			AmqpChannel: channel,
		}, nil
	})
}

func (c *Connection) ReleaseChannel(channel *Channel) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.strategy.UpdateByRelease(channel.Id)
}

func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.AmqpConnection.Close()
	if err != nil {
		return fmt.Errorf("conn close: %w", err)
	}
	return nil
}

// channel

type Channel struct {
	Id     int
	Parent *Connection
	*AmqpChannel
}

func (ch *Channel) CreateQueue(useParam ...UseQueueParam) (queueName string, err error) {
	// https://pkg.go.dev/github.com/rabbitmq/amqp091-go@v1.1.0#pkg-variables
	// https://github.com/rabbitmq/amqp091-go/issues/170
	parent := ch.Parent
	parent.mu.Lock()
	defer ch.Parent.mu.Unlock()

	var ex AmqpExchangeDeclareParam
	var q AmqpQueueDeclareParam
	var bind AmqpQueueBindParam
	for _, replace := range useParam {
		replace(&ex, &q, &bind)
	}

	if err := ch.ExchangeDeclare(
		ex.ExchangeName,
		ex.ExchangeKind,
		ex.Durable,
		ex.AutoDelete,
		ex.Internal,
		ex.NoWait,
		ex.Args,
	); err != nil {
		return "", fmt.Errorf("declare exchange : %v", err)
	}

	queue, err := ch.QueueDeclare(
		q.QueueName,
		q.Durable,
		q.AutoDelete,
		q.Exclusive,
		q.NoWait,
		q.Args,
	)
	if err != nil {
		return "", fmt.Errorf("declare queue : %v", err)
	}

	if err = ch.QueueBind(
		queue.Name,
		bind.Key,
		bind.ExchangeName,
		bind.NoWait,
		bind.Args,
	); err != nil {
		return "", fmt.Errorf("bind key: %v", err)
	}

	return queue.Name, nil
}

func (ch *Channel) CreateConsumer(queueName string, consumerName string, fn ConsumerFunc, useParam ...UseConsumerParam) (*Consumer, error) {
	ch.Parent.mu.Lock()
	defer ch.Parent.mu.Unlock()
	return newConsumer(queueName, consumerName, ch, fn, useParam...)
}

func (ch *Channel) CreateConsumers(queueName string, consumerName string, consumerQty int, fn ConsumerFunc, useParam ...UseConsumerParam) ([]*Consumer, error) {
	// https://github.com/rabbitmq/amqp091-go/issues/170
	ch.Parent.mu.Lock()
	defer ch.Parent.mu.Unlock()

	var consumers []*Consumer
	for i := 0; i < consumerQty; i++ {
		consumerId := consumerName + strconv.Itoa(i)
		consumer, err := newConsumer(queueName, consumerId, ch, fn, useParam...)
		if err != nil {
			return nil, err
		}
		consumers = append(consumers, consumer)
	}

	return consumers, nil
}
