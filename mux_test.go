package rabbitHalo

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClaimTask_apply_handlers_for_ByFanout_and_ByTopic_success(t *testing.T) {
	// arrange
	mux := NewMessageMux()
	var recordTasks []string

	bindingKey1 := "system_design"
	fanoutHandler1 := func(ctx context.Context, msg *AmqpMessage) error {
		if msg.RoutingKey != bindingKey1 {
			return nil
		}
		ClaimTask(ctx)

		recordTasks = append(recordTasks, msg.RoutingKey+"_ok!")
		return nil
	}
	mux.RegisterConsumerFuncByFanout(bindingKey1, fanoutHandler1)

	bindingKey2 := "database"
	fanoutHandler2 := func(ctx context.Context, msg *AmqpMessage) error {
		if msg.RoutingKey != bindingKey2 {
			return nil
		}
		ClaimTask(ctx)

		recordTasks = append(recordTasks, msg.RoutingKey+"_ok!")
		return nil
	}
	mux.RegisterConsumerFuncByFanout(bindingKey2, fanoutHandler2)

	bindingKey3 := "order.*.user"
	routingKey3a := "order.notebook:'xps'.user"
	routingKey3b := "order.notebook:'m1'.user"
	wildcardTopicHandler3 := func(ctx context.Context, msg *AmqpMessage) error {
		if msg.RoutingKey != routingKey3a {
			return nil
		}
		ClaimTask(ctx)

		recordTasks = append(recordTasks, msg.RoutingKey+"_ok!")
		return nil
	}
	mux.RegisterConsumerFuncByTopic(bindingKey3, wildcardTopicHandler3)

	bindingKey4 := ""
	routingKey4 := "leetcode"
	fanoutHandler4 := func(ctx context.Context, msg *AmqpMessage) error {
		if msg.RoutingKey != routingKey4 {
			return nil
		}
		ClaimTask(ctx)

		recordTasks = append(recordTasks, msg.RoutingKey+"_ok!")
		return nil
	}
	mux.RegisterConsumerFuncByFanout(bindingKey4, fanoutHandler4)

	bindingKey5 := "gopher"
	fanoutHandler5 := func(ctx context.Context, msg *AmqpMessage) error {
		if msg.RoutingKey != bindingKey5 {
			return nil
		}
		ClaimTask(ctx)

		recordTasks = append(recordTasks, msg.RoutingKey+"_ok!")
		return nil
	}
	mux.RegisterConsumerFunc(bindingKey5, fanoutHandler5)

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
		{ConsumerTag: "mock_ClaimTask", RoutingKey: bindingKey2},  // match
		{ConsumerTag: "mock_ClaimTask", RoutingKey: routingKey3a}, // match
		{ConsumerTag: "mock_ClaimTask", RoutingKey: routingKey4},  // match
		{ConsumerTag: "mock_ClaimTask", RoutingKey: bindingKey1},  // match
		{ConsumerTag: "mock_ClaimTask", RoutingKey: "random_key2"},
		{ConsumerTag: "mock_ClaimTask", RoutingKey: bindingKey5}, // match
		{ConsumerTag: "mock_ClaimTask", RoutingKey: routingKey3b},
		{ConsumerTag: "mock_ClaimTask", RoutingKey: bindingKey1}, // match
		{ConsumerTag: "mock_ClaimTask", RoutingKey: "random_key3"},
		{ConsumerTag: "mock_ClaimTask", RoutingKey: bindingKey2}, // match
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

func TestMessageMux_RegisterConsumerFuncByFanout_duplicate_handler_register_and_remove_success(t *testing.T) {
	// arrange
	mux := NewMessageMux()

	key := "caesar"
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
	mux.RegisterConsumerFuncByFanout(key, fn)
	mux.RemoveConsumerFuncByFanout(key)

	mux.RegisterConsumerFuncByFanout(key, fn)
	mux.RemoveConsumerFuncByFanout(key)

	mux.RegisterConsumerFuncByFanout(key, fn)
}
