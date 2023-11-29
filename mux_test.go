package rabbitHalo

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStopNextFunc_RegisterConsumerFastFunc_and_RegisterConsumerSlowFunc_success(t *testing.T) {
	// arrange
	mux := NewMessageMux()
	var recordTasks []string

	key1 := NewKeyTopic("system_design", "system_design")
	mux.RegisterConsumerFastHook(key1, func(ctx context.Context, msg *AmqpMessage) error {
		recordTasks = append(recordTasks, msg.RoutingKey+"_ok!")
		return nil
	})

	key2 := NewKeyDirect("database")
	mux.RegisterConsumerFastHook(key2, func(ctx context.Context, msg *AmqpMessage) error {
		recordTasks = append(recordTasks, msg.RoutingKey+"_ok!")
		return nil
	})

	bindingKey3 := "order.*.user"
	routingKey3a := "order.notebook:'xps'.user"
	routingKey3b := "order.notebook:'m1'.user"
	key3 := NewKeyTopic(routingKey3a, bindingKey3)
	mux.RegisterConsumerSlowHook(key3, func(ctx context.Context, msg *AmqpMessage) error {
		if msg.RoutingKey != key3.RoutingKey() {
			return nil
		}
		AckMessage(ctx)

		recordTasks = append(recordTasks, msg.RoutingKey+"_ok!")
		return nil
	})

	key4 := NewKeyFanout("leetcode")
	mux.RegisterConsumerSlowHook(key4, func(ctx context.Context, msg *AmqpMessage) error {
		AckMessage(ctx)
		recordTasks = append(recordTasks, msg.RoutingKey+"_ok!")
		return nil
	})

	key5 := NewKeyDirect("gopher")
	mux.RegisterConsumerFastHook(key5, func(ctx context.Context, msg *AmqpMessage) error {
		recordTasks = append(recordTasks, msg.RoutingKey+"_ok!")
		return nil
	})

	var actualNotMatchKeys []string
	mux.AddConsumerErrorChain(func(next ConsumerFunc) ConsumerFunc {
		return func(ctx context.Context, msg *AmqpMessage) error {
			err := next(ctx, msg)
			if err == nil {
				return nil
			}

			switch {
			case errors.Is(err, ErrNotFoundHandler):
				msg.Ack(false)
				actualNotMatchKeys = append(actualNotMatchKeys, msg.RoutingKey)
				return err

			default:
				msg.Ack(false)
				defaultLogger.Error(
					"default error handle: msgType=%q: consumer=%q: key=%q: payload=%q: %v",
					msg.Type, msg.ConsumerTag, msg.RoutingKey, msg.Body, err,
				)
				return err
			}
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
	mux.RegisterConsumerFastHook(key, fn)
	mux.RemoveConsumerFastHook(key)

	mux.RegisterConsumerFastHook(key, fn)
	mux.RemoveConsumerFastHook(key)

	mux.RegisterConsumerFastHook(key, fn)
}
