package rabbitHalo

import "sync"

// NewClient 有 connectionMaxQty 個 connection , 每個 conn 有 channelMaxQty 個 channel,
// 因此總數量是 connectionMaxQty * channelMaxQty
func NewClient(amqpUri string, connectionMaxQty, channelMaxQty int) (*Client, error) {
	client := &Client{
		connectionAll: make([]*Connection, connectionMaxQty),
		strategy:      NewMinUsageRateStrategy(connectionMaxQty),
	}
	newConn := func(connId int) (*Connection, error) {
		connection, err := newConnection(amqpUri, connId, channelMaxQty, client)
		if err != nil {
			return nil, err
		}
		client.strategy.SetChildStrategy(connection.Id, connection.strategy)
		return connection, err
	}
	client.newConnection = newConn

	_, err := client.AcquireConnection()
	if err != nil {
		return nil, err
	}
	return client, nil
}

type Client struct {
	mu            sync.Mutex
	connectionAll []*Connection
	strategy      *MinUsageRateStrategy
	newConnection func(connId int) (*Connection, error)
}

func (client *Client) AcquireConnection() (conn *Connection, err error) {
	client.mu.Lock()
	defer func() {
		minIndex, totalScore, listQty := client.strategy.ViewUsageQty()
		defaultLogger.Info("get connection=%v: min=%v totalScore=%v qty=%v\n", conn.Id, minIndex, totalScore, listQty)
		client.mu.Unlock()
	}()
	return lazyNewResource(client.strategy, client.connectionAll, client.newConnection)
}

func (client *Client) ReleaseConnection(conn *Connection) {
	client.mu.Lock()
	client.strategy.UpdateByRelease(conn.Id)
	client.mu.Unlock()
}

func (client *Client) CloseClient() error {
	for _, connection := range client.connectionAll {
		conn := connection
		go func() {
			conn.CloseConnection()
		}()
	}
	return nil
}
