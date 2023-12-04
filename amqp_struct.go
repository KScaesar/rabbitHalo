package rabbitHalo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func newConnection(amqpUri string, connId int, channelMaxQty int, client *Client) (*Connection, error) {
	var amqpConnection *AmqpConnection

	retryMaxTimeForFirstCreate := 30 * time.Second
	err := retry(retryMaxTimeForFirstCreate, "dial connection", func() (err error) {
		amqpConnection, err = amqp.Dial(amqpUri)
		return
	})
	if err != nil {
		return nil, err
	}

	mqCloseCtx, cancelFunc := context.WithCancel(context.Background())

	conn := &Connection{
		Id:           connId,
		channelAll:   make([]*Channel, channelMaxQty),
		strategy:     NewMinUsageRateStrategy(channelMaxQty),
		parent:       client,
		amqpConn:     amqpConnection,
		mqClose:      mqCloseCtx,
		commandClose: cancelFunc,
	}

	conn.unlimitedRetryAmqpConnection = func(ctx context.Context) {
		var retryUnlimitedTimeForRecovery time.Duration = 0
		retry(retryUnlimitedTimeForRecovery, "retryAmqpConnection", func() error {
			select {
			case <-ctx.Done():
				return nil
			default:
				conn.mu.Lock()
				defer conn.mu.Unlock()

				if conn.isClosed.Load() {
					return nil
				}

				amqpConn, err := amqp.Dial(amqpUri)
				if err != nil {
					return err
				}
				conn.amqpConn = amqpConn
			}
			return nil
		})
	}

	go conn.monitorConnection()
	return conn, nil
}

// connection

type Connection struct {
	mu         sync.Mutex
	Id         int
	channelAll []*Channel
	strategy   *MinUsageRateStrategy
	parent     *Client

	amqpConn                     *AmqpConnection
	unlimitedRetryAmqpConnection func(ctx context.Context)
	isClosed                     atomic.Bool

	mqClose      context.Context
	commandClose func()
}

func (conn *Connection) AmqpConnection() *AmqpConnection {
	return conn.amqpConn
}

func (conn *Connection) monitorConnection() {
	for {
		defaultLogger.Debug("conn=%v monitor loop", conn.Id)
		err := <-conn.amqpConn.NotifyClose(make(chan *AmqpError, 1))
		if err == nil {
			defaultLogger.Info("rabbit connection close success")
			return
		}
		defaultLogger.Error("rabbit connection=%v close: %v", conn.Id, err)
		conn.unlimitedRetryAmqpConnection(conn.mqClose)
		defaultLogger.Info("retry amqp connection success")
	}
}

func (conn *Connection) CloseConnection() error {
	if conn.isClosed.Load() {
		return nil
	}

	conn.isClosed.Store(true)
	conn.commandClose()
	defaultLogger.Info("connection=%v close success", conn.Id)

	conn.mu.Lock()
	defer conn.mu.Unlock()

	err := conn.amqpConn.Close()
	if err != nil {
		return fmt.Errorf("connection close: %w", err)
	}
	return nil
}

func (conn *Connection) AcquireChannel() (ch *Channel, err error) {
	if conn.isClosed.Load() {
		return nil, ErrResourceClosed
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()

	return lazyNewResource(conn.strategy, conn.channelAll, func(id int) (*Channel, error) {
		channel, err := newChannel(id, conn)
		if err != nil {
			return nil, err
		}
		conn.parent.strategy.SetChildStrategy(conn.Id, conn.strategy)
		return channel, nil
	})
}

func (c *Connection) ReleaseChannel(channel *Channel) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.strategy.UpdateByRelease(channel.Id)
}

// channel

func newChannel(channelId int, conn *Connection) (*Channel, error) {
	amqpChannel, err := conn.amqpConn.Channel()
	if err != nil {
		return nil, err
	}

	mqCloseCtx, cancelFunc := context.WithCancel(conn.mqClose)

	channel := &Channel{
		Id:           channelId,
		amqpCh:       amqpChannel,
		isClosed:     atomic.Bool{},
		parent:       conn,
		mqClose:      mqCloseCtx,
		commandClose: cancelFunc,
	}

	channel.unlimitedRetryAmqpChannel = func(ctx context.Context) {
		var unlimited time.Duration = 0
		retry(unlimited, "retryAmqpChannel", func() error {
			select {
			case <-ctx.Done():
				return nil
			default:
				channel.parent.mu.Lock()
				defer channel.parent.mu.Unlock()

				if channel.isClosed.Load() {
					return nil
				}

				amqpCh, err := channel.parent.amqpConn.Channel()
				if err != nil {
					return err
				}
				channel.amqpCh = amqpCh
			}
			return nil
		})
		return
	}

	go channel.monitorChannel()
	return channel, nil
}

type Channel struct {
	Id                        int
	mu                        sync.Mutex
	amqpCh                    *AmqpChannel
	unlimitedRetryAmqpChannel func(ctx context.Context)
	isClosed                  atomic.Bool

	// https://pkg.go.dev/github.com/rabbitmq/amqp091-go@v1.1.0#pkg-variables
	// https://github.com/rabbitmq/amqp091-go/issues/170
	parent *Connection

	mqClose      context.Context
	commandClose func()
}

func (ch *Channel) monitorChannel() {
	for {
		defaultLogger.Debug("channel=%v monitor loop", ch.Id)
		select {
		case Err := <-ch.amqpCh.NotifyClose(make(chan *AmqpError, 1)):
			if Err == nil {
				return
			}
			defaultLogger.Error("rabbit channel=%v close: %v", ch.Id, Err)
			ch.unlimitedRetryAmqpChannel(ch.mqClose)
			defaultLogger.Info("retry rabbit channel success")
		}
	}
}

func (ch *Channel) CloseChannel() error {
	if ch.isClosed.Load() {
		return nil
	}
	ch.isClosed.Store(true)
	ch.commandClose()
	defaultLogger.Info("channel=%v close success", ch.Id)

	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.amqpCh.Close()
}

func (ch *Channel) CreateAmqpExchange(ex *AmqpExchangeDeclareParam) error {
	if ch.isClosed.Load() {
		return ErrResourceClosed
	}
	ch.parent.mu.Lock()
	defer ch.parent.mu.Unlock()

	err := ch.amqpCh.ExchangeDeclare(
		ex.ExchangeName,
		ex.ExchangeKind,
		ex.Durable,
		ex.AutoDelete,
		ex.Internal,
		ex.NoWait,
		ex.Args,
	)
	if err != nil {
		return fmt.Errorf("declare exchange=%q: %w", ex.ExchangeName, err)
	}
	return nil
}

func (ch *Channel) CreateAmqpQueue(useParam ...UseAmqpQueueParam) (AmqpQueue, error) {
	ch.parent.mu.Lock()
	defer ch.parent.mu.Unlock()

	type response struct {
		q   AmqpQueue
		err error
	}
	asyncTask := make(chan response, 1)
	go func() {
		// prod 出現一種情境, 第三方套件阻塞停止在 amqpChannel.QueueDeclare(), 也不返回錯誤
		// 造成 naming 伺服器看似正常運作, 但是其實初始化 naming 應該視為失敗的
		//
		// 因此另外撰寫 timeout 邏輯, 避免此類情況再次發生
		// 後續追查原因, 只知道是 prod 環境 rabbitmq 有問題, 但不知道是如何解決的
		//
		// 測試環境無法復現此情況
		// 測試環境的 mq 是單點架構, prod 環境是集群架構
		queue, err := ch.createAmqpQueue(useParam...)
		asyncTask <- response{queue, err}
	}()

	ctxWithTimeout, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()

	select {
	case <-ctxWithTimeout.Done():
		return AmqpQueue{}, errors.New("rabbitmq server is experiencing issue, causing 'CreateAmqpQueue' operation to be blocked and not returning")
	case resp := <-asyncTask:
		return resp.q, resp.err
	}
}

func (ch *Channel) createAmqpQueue(useParam ...UseAmqpQueueParam) (AmqpQueue, error) {
	if ch.isClosed.Load() {
		return AmqpQueue{}, ErrResourceClosed
	}

	var q AmqpQueueDeclareParam
	var bind AmqpQueueBindParam
	for _, replace := range useParam {
		replace(&q, &bind)
	}

	queue, err := ch.amqpCh.QueueDeclare(
		q.QueueName,
		q.Durable,
		q.AutoDelete,
		q.Exclusive,
		q.NoWait,
		q.Args,
	)
	if err != nil {
		return AmqpQueue{}, fmt.Errorf("declare queue=%q: %v", q.QueueName, err)
	}

	if err = ch.amqpCh.QueueBind(
		queue.Name,
		bind.Key,
		bind.ExchangeName,
		bind.NoWait,
		bind.Args,
	); err != nil {
		return AmqpQueue{}, fmt.Errorf("bindingkey=%q: %v", bind.Key, err)
	}

	return queue, nil
}

func (ch *Channel) CreateConsumer(fn ConsumerFunc, useParam ...UseAmqpConsumerParam) (*Consumer, error) {
	if ch.isClosed.Load() {
		return nil, ErrResourceClosed
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return newConsumer(ch, fn, useParam...)
}
