package flow

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type ErrorFunc func() error
type chainTerminator struct {
	message string
	logger  *zap.Logger
	args    []interface{}
}

func (p *chainTerminator) Error() string {
	return p.message
}

func (p *chainTerminator) publishMessage() {
	p.logger.Sugar().Infof(p.message, p.args...)
}

func TerminateChain(logger *zap.Logger, msg string, args ...interface{}) error {
	return &chainTerminator{
		message: msg,
		logger:  logger,
		args:    args,
	}
}

func WrappedChainFunc(message string, fn ErrorFunc) ErrorFunc {
	return func() error {
		err := fn()
		if chainTerm, ok := err.(*chainTerminator); ok {
			chainTerm.publishMessage()
			return nil
		}

		return errors.Wrap(err, message)
	}
}

func Chain(fns ...func() error) error {
	for _, fn := range fns {
		if err := fn(); err != nil {
			if chainTerm, ok := err.(*chainTerminator); ok {
				chainTerm.publishMessage()
				return nil
			}

			return err
		}
	}

	return nil
}

func ChainWraped(fns ...func() (string, error)) error {
	for _, fn := range fns {
		if msg, err := fn(); err != nil {
			return errors.Wrap(err, msg)
		}
	}

	return nil
}

func SleepWithContext(ctx context.Context, sleep time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(sleep):
	}
}

func SleepUntilWithContext(ctx context.Context, until time.Time) {
	timer := time.NewTimer(time.Until(until))
	defer timer.Stop()

	select {
	case <-timer.C:
		return
	case <-ctx.Done():
		return
	}
}
