package rabbitHalo

import (
	"fmt"
)

func NewTopologyRebuilder(ch *Channel) TopologyRebuilder {
	return TopologyRebuilder{
		channel: ch,
	}
}

// TopologyRebuilder
// 保存使用者的路由建構邏輯, 以便於斷線後可以進行路由拓墣重建,
// 因為並非所有 queue, exchange 都是設定為永久.
//
// 在簡單個使用案例中, 也可以看成是優化開發體驗的工具,
// 可以少寫一些 err != nil, 比如: QuicklyXXX function.
type TopologyRebuilder struct {
	channel     *Channel
	err         error
	reAnyAction []func(*Channel) error
}

func (builder TopologyRebuilder) BuildAny(action func(*Channel) error) TopologyRebuilder {
	if builder.err != nil {
		return builder
	}

	err := action(builder.channel)
	if err != nil {
		builder.err = err
		return builder
	}

	builder.reAnyAction = append(builder.reAnyAction, action)
	return builder
}

func (builder TopologyRebuilder) QuicklyBuildExchange(ex *AmqpExchangeDeclareParam) TopologyRebuilder {
	return builder.BuildAny(func(ch *Channel) error {
		return ch.CreateAmqpExchange(ex)
	})
}

func (builder TopologyRebuilder) QuicklyBuildQueue(useParam ...UseAmqpQueueParam) TopologyRebuilder {
	return builder.BuildAny(func(ch *Channel) error {
		_, err := ch.CreateAmqpQueue(useParam...)
		return err
	})
}

// rebuildAction 重建路由邏輯的主要函數
//
// 呼叫端注意:
// 各個 reAction 通常會包含上鎖邏輯, 比如: Channel.CreateAmqpQueue, Channel.CreateAmqpExchange.
// 小心不要重複呼叫 mu.Lock, 會產生 deadlock
func (builder TopologyRebuilder) rebuildAction(ch *Channel) error {
	if len(builder.reAnyAction) != 0 {
		for _, action := range builder.reAnyAction {
			err := action(ch)
			if err != nil {
				return fmt.Errorf("reAnyAction: %w", err)
			}
		}
	}
	return nil
}

func (builder TopologyRebuilder) BuildConsumer(fn ConsumerFunc, useParam ...UseAmqpConsumerParam) (*Consumer, error) {
	if builder.err != nil {
		return nil, builder.err
	}

	consumer, err := builder.channel.CreateConsumer(fn, useParam...)
	if err != nil {
		builder.err = err
		return nil, err
	}

	consumer.setRebuilder(&builder)
	return consumer, nil
}
