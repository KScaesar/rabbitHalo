package rabbitHalo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConsumerChain_Link_confirm_the_execution_order_of_decorators(t *testing.T) {
	// arrange
	actualOutput := &bytes.Buffer{}
	fmt.Fprintf(actualOutput, "\n")

	errChain := []ConsumerDecorator{
		func(next ConsumerFunc) ConsumerFunc {
			return func(ctx context.Context, msg *AmqpMessage) error {
				fmt.Fprintf(actualOutput, "err func before\n")
				err := next(ctx, msg)
				if err != nil {
					fmt.Fprintf(actualOutput, "handle err: %v\n", err)
					return nil
				}
				fmt.Fprintf(actualOutput, "err func after\n")
				return nil
			}

		},
	}

	featChain := []ConsumerDecorator{
		func(next ConsumerFunc) ConsumerFunc {
			return func(ctx context.Context, msg *AmqpMessage) error {
				fmt.Fprintf(actualOutput, "feature1 before\n")
				err := next(ctx, msg)
				fmt.Fprintf(actualOutput, "feature1 after\n")
				return err
			}
		},
		func(next ConsumerFunc) ConsumerFunc {
			return func(ctx context.Context, msg *AmqpMessage) error {
				fmt.Fprintf(actualOutput, "feature2 before\n")
				err := next(ctx, msg)
				fmt.Fprintf(actualOutput, "feature2 after\n")
				return err
			}
		},
	}

	chain := NewConsumerChain(errChain, featChain)

	taskHandler := func(ctx context.Context, msg *AmqpMessage) error {
		fmt.Fprintf(actualOutput, "execute task\n")
		return errors.New("db connect fail")
	}

	// expected
	expectedOutput := `
err func before
feature1 before
feature2 before
execute task
feature2 after
feature1 after
handle err: db connect fail
`

	// action
	err := chain.Link(taskHandler)(nil, nil)

	// assert
	assert.NoError(t, err)
	assert.Equal(t, actualOutput.String(), expectedOutput)
}
