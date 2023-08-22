package dopushack

import "context"

// IsOpen
// Beware, this function consumes an item from the channel. Use only on channels without
// data, i.e. which are only closed but not written into.
func IsOpen(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	default:
		return true
	}
}

func Send[T any](ctx context.Context, workCh chan<- T, val T) bool {
	select {
	case <-ctx.Done():
		return false
	case workCh <- val:
		return true
	}
}

func Receive[T any](ctx context.Context, workCh <-chan T, onReceive func(t T) bool) bool {
	select {
	case <-ctx.Done():
		return false
	case t := <-workCh:
		return onReceive(t)
	}
}
