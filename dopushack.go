package dopushack

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

type DoFunc[TIn any, TOut any] func(TIn) TOut
type PushFunc[TIn any, TOut any] func(TIn, TOut) chan error
type AckFunc[TIn any, TOut any] func(in TIn, res TOut, pushSt time.Time, err error)

type ParseQueue[TIn any, TOut any] struct {
	ctx    context.Context
	cancel func()

	wg sync.WaitGroup

	pushCh chan parseTask[TIn, TOut]
	ackCh  chan pushTask[TIn, TOut]

	logger log.Logger

	pushF  PushFunc[TIn, TOut]
	parseF DoFunc[TIn, TOut]
	ackF   AckFunc[TIn, TOut]
}

type pushTask[TIn any, TOut any] struct {
	errCh  chan error
	in     TIn
	res    TOut
	pushSt time.Time
}

type parseTask[TIn any, TOut any] struct {
	in    TIn
	resCh chan TOut
}

// Add will schedule new message parse
//
//	Do not call concurrently with Close()!
func (p *ParseQueue[TIn, TOut]) Add(message TIn) error {
	if !IsOpen(p.ctx) {
		return errors.New("parser q is already closed")
	}
	p.pushCh <- p.makeParseTask(message)
	return nil
}

// Close shutdown all goroutines
//
//	Do not call concurrently with Add()
func (p *ParseQueue[TIn, TOut]) Close() {
	p.cancel()
	p.wg.Wait()
}

func (p *ParseQueue[TIn, TOut]) makeParseTask(items TIn) parseTask[TIn, TOut] {
	resCh := make(chan TOut)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		parseResult := p.parseF(items)
		Send(p.ctx, resCh, parseResult)
	}()
	return parseTask[TIn, TOut]{in: items, resCh: resCh}
}

func (p *ParseQueue[TIn, TOut]) pushLoop() {
	defer p.wg.Done()
	for Receive(p.ctx, p.pushCh, func(parsed parseTask[TIn, TOut]) bool {
		return Receive(p.ctx, parsed.resCh, func(items TOut) bool {
			return Send(p.ctx, p.ackCh, pushTask[TIn, TOut]{errCh: p.pushF(parsed.in, items), in: parsed.in, pushSt: time.Now()})
		})
	}) {
	}
}

func (p *ParseQueue[TIn, TOut]) ackLoop() {
	defer p.wg.Done()
	for Receive(p.ctx, p.ackCh, func(ack pushTask[TIn, TOut]) bool {
		return Receive(p.ctx, ack.errCh, func(err error) bool {
			p.ackF(ack.in, ack.res, ack.pushSt, err)
			return true
		})
	}) {
	}
}

// New construct new do-push-ack chain worker chain
// `Add` trigger a parallel `do` calls, which then passes to `push` func in same order as was in `Add`.
func New[TIn any, TOut any](
	ctx context.Context,
	parallelism int,
	pushF PushFunc[TIn, TOut],
	parseF DoFunc[TIn, TOut],
	ackF AckFunc[TIn, TOut],
) *ParseQueue[TIn, TOut] {
	if parallelism < 2 {
		parallelism = 2
	}
	cctx, cancel := context.WithCancel(ctx)
	item := &ParseQueue[TIn, TOut]{
		wg: sync.WaitGroup{},

		ctx:    cctx,
		cancel: cancel,

		// I'm too lazy to explain why -2, but oleg - not:
		// Let's say you've set the parallelism to 10. The first time you call Add(),
		// a parsing goroutine is created, and a task to read the parsing result is placed into a channel.
		// Since pushLoop is not busy, it immediately reads this task from the channel
		// and waits during the read operation from resCh. At this point, there are 8 spots left in the pushCh
		// channel (10 - 2 == 8).
		//
		// Afterwards, you call Add() 8 more times, and your pushCh channel becomes filled with tasks.
		// So far, you've made 9 calls in total.
		//
		// Then, you call Add() again. The makeParseTask function is invoked, and a parsing goroutine starts.
		// However, we get blocked while writing to the pushCh channel, as there are already 8 buffered messages.
		// In total, this adds up to 8 + 2 = 10, which matches the code logic.
		//
		// This is a bit of a messy situation, indeed.
		pushCh: make(chan parseTask[TIn, TOut], parallelism-2),
		ackCh:  make(chan pushTask[TIn, TOut]),

		pushF:  pushF,
		parseF: parseF,
		ackF:   ackF,
	}
	item.wg.Add(2)
	go item.pushLoop()
	go item.ackLoop()
	return item
}
