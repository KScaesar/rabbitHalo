package rabbitHalo

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStopNextFunc_RegisterConsumerFunc_and_RegisterConsumerFuncByDynamic_success(t *testing.T) {
	// arrange
	mux := NewMessageMux()
	var recordTasks []string

	key1 := NewKeyTopic("system_design", "system_design")
	mux.RegisterConsumerFunc(key1, func(ctx context.Context, msg *AmqpMessage) error {
		recordTasks = append(recordTasks, msg.RoutingKey+"_ok!")
		return nil
	})

	key2 := NewKeyDirect("database")
	mux.RegisterConsumerFunc(key2, func(ctx context.Context, msg *AmqpMessage) error {
		recordTasks = append(recordTasks, msg.RoutingKey+"_ok!")
		return nil
	})

	bindingKey3 := "order.*.user"
	routingKey3a := "order.notebook:'xps'.user"
	routingKey3b := "order.notebook:'m1'.user"
	key3 := NewKeyTopic(DynamicRoutingKey, bindingKey3)
	mux.RegisterConsumerFuncByDynamic(key3, func(ctx context.Context, msg *AmqpMessage) error {
		if msg.RoutingKey != routingKey3a {
			return nil
		}
		MuxStopNext(ctx)

		recordTasks = append(recordTasks, msg.RoutingKey+"_ok!")
		return nil
	})

	key4 := NewKeyFanout("leetcode")
	mux.RegisterConsumerFunc(key4, func(ctx context.Context, msg *AmqpMessage) error {
		recordTasks = append(recordTasks, msg.RoutingKey+"_ok!")
		return nil
	})

	key5 := NewKeyDirect("gopher")
	mux.RegisterConsumerFunc(key5, func(ctx context.Context, msg *AmqpMessage) error {
		recordTasks = append(recordTasks, msg.RoutingKey+"_ok!")
		return nil
	})

	var actualNotMatchKeys []string
	mux.AddConsumerErrorChain(func(next ConsumerFunc) ConsumerFunc {
		return func(ctx context.Context, msg *AmqpMessage) error {
			err := next(ctx, msg)
			if err != nil {
				if errors.Is(err, ErrNotMatchRoutingKey) {
					actualNotMatchKeys = append(actualNotMatchKeys, msg.RoutingKey)
					return nil
				}
				defaultLogger.Error("error msg: %v", err)
				return nil
			}
			return nil
		}
	})

	fakeData := []AmqpMessage{
		{ConsumerTag: "mock_ClaimTask", RoutingKey: "random_key1"},
		{ConsumerTag: "mock_ClaimTask", RoutingKey: key2.RoutingKey()}, // match
		{ConsumerTag: "mock_ClaimTask", RoutingKey: routingKey3a},      // match
		{ConsumerTag: "mock_ClaimTask", RoutingKey: key4.RoutingKey()}, // match
		{ConsumerTag: "mock_ClaimTask", RoutingKey: key1.RoutingKey()}, // match
		{ConsumerTag: "mock_ClaimTask", RoutingKey: "random_key2"},
		{ConsumerTag: "mock_ClaimTask", RoutingKey: key5.RoutingKey()}, // match
		{ConsumerTag: "mock_ClaimTask", RoutingKey: routingKey3b},
		{ConsumerTag: "mock_ClaimTask", RoutingKey: key1.RoutingKey()}, // match
		{ConsumerTag: "mock_ClaimTask", RoutingKey: "random_key3"},
		{ConsumerTag: "mock_ClaimTask", RoutingKey: key2.RoutingKey()}, // match
	}
	ctx := context.Background()

	// expect
	expectedTasks := []string{
		"database_ok!",
		"order.notebook:'xps'.user_ok!",
		"leetcode_ok!",
		"system_design_ok!",
		"gopher_ok!",
		"system_design_ok!",
		"database_ok!",
	}
	expectedNotMatchKeys := []string{
		"random_key1",
		"random_key2",
		routingKey3b,
		"random_key3",
	}

	// action
	for _, msg := range fakeData {
		mux.ServeConsume(ctx, &msg)
	}

	// assert
	assert.Equal(t, expectedTasks, recordTasks, "check tasks")
	assert.Equal(t, expectedNotMatchKeys, actualNotMatchKeys, "check miss keys")
}

func TestMessageMux_RegisterConsumerFunc_duplicate_handler_register_and_remove_success(t *testing.T) {
	// arrange
	mux := NewMessageMux()

	key := NewKeyFanout("caesar")
	fn := func(ctx context.Context, msg *AmqpMessage) error { return nil }

	actualResponse := ""
	expectedResponse := ""
	defer func() {
		result := recover()
		if result != nil {
			actualResponse = result.(string)
		}

		// assert
		assert.Equal(t, expectedResponse, actualResponse)
	}()

	// action
	mux.RegisterConsumerFunc(key, fn)
	mux.RemoveConsumerFunc(key)

	mux.RegisterConsumerFunc(key, fn)
	mux.RemoveConsumerFunc(key)

	mux.RegisterConsumerFunc(key, fn)
}
