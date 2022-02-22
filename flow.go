package flow

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"gopkg.in/retry.v1"
)

var (
	errRetry = errors.New("retry error")
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

func TerminateAll(logger *zap.Logger, msg string, args ...interface{}) error {
	return &chainTerminator{
		message: msg,
		logger:  logger,
		args:    args,
	}
}

func Retry() error {
	return errRetry
}

func WrappedStep(message string, fn ErrorFunc) ErrorFunc {
	return func() error {
		return errors.Wrap(fn(), message)
	}
}

func Parallel(fns ...ErrorFunc) error {
	group := new(errgroup.Group)
	for _, fn := range fns {
		group.Go(fn)
	}

	if err := group.Wait(); err != nil {
		return handleTerminator(err)
	}
	return nil
}

func RetryableStep(strategy retry.Strategy, fn ErrorFunc) ErrorFunc {
	return func() error {
		for a := retry.Start(strategy, nil); a.Next(); {
			if err := fn(); err != nil {
				if errors.Is(err, errRetry) {
					continue
				}
				return err
			}
		}

		return nil
	}
}

func ParallelWithContext(ctx context.Context, fns ...ErrorFunc) error {
	group, _ := errgroup.WithContext(ctx)
	for _, fn := range fns {
		group.Go(fn)
	}

	if err := group.Wait(); err != nil {
		return handleTerminator(err)
	}
	return nil
}

func ParallelStep(fns ...ErrorFunc) ErrorFunc {
	return func() error {
		group := new(errgroup.Group)
		for _, fn := range fns {
			group.Go(fn)
		}

		if err := group.Wait(); err != nil {
			return handleTerminator(err)
		}
		return nil
	}
}

func ParallelStepWithContext(ctx context.Context, fns ...ErrorFunc) ErrorFunc {
	return func() error {
		group, _ := errgroup.WithContext(ctx)
		for _, fn := range fns {
			group.Go(fn)
		}

		if err := group.Wait(); err != nil {
			return handleTerminator(err)
		}
		return nil
	}
}

func Sequence(fns ...ErrorFunc) error {
	for _, fn := range fns {
		if err := fn(); err != nil {
			return handleTerminator(err)
		}
	}

	return nil
}

func SequenceWithContext(ctx context.Context, fns ...ErrorFunc) error {
	for _, fn := range fns {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err := fn(); err != nil {
			return handleTerminator(err)
		}
	}

	return nil
}

func SequenceStep(fns ...ErrorFunc) ErrorFunc {
	return func() error {
		for _, fn := range fns {
			if err := fn(); err != nil {
				return handleTerminator(err)
			}
		}

		return nil
	}
}

func SequenceStepWithContext(ctx context.Context, fns ...ErrorFunc) ErrorFunc {
	return func() error {
		for _, fn := range fns {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			if err := fn(); err != nil {
				return handleTerminator(err)
			}
		}

		return nil
	}
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

func handleTerminator(err error) error {
	if chainTerm, ok := errors.Cause(err).(*chainTerminator); ok {
		chainTerm.publishMessage()
		return nil
	}

	return err
}
