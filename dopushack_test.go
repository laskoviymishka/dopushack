package dopushack

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAckOrder(t *testing.T) {
	wgMap := map[int]*sync.WaitGroup{}
	parallelism := 10
	for i := 0; i < parallelism*10; i++ {
		wgMap[i] = &sync.WaitGroup{}
		wgMap[i].Add(1)
	}
	var res []int
	mu := sync.Mutex{}
	inflight := atomic.Int32{}
	q := New[int, int](context.Background(), parallelism, func(i int, i2 int) chan error {
		return nil
	}, func(data int) int {
		inflight.Add(1)
		defer inflight.Add(-1)
		wgMap[data].Wait()
		return data
	}, func(data int, _ int, _ time.Time, _ error) {
		mu.Lock()
		defer mu.Unlock()
		res = append(res, data)
	})
	// all 10 parse func must be called
	go func() {
		for i := 0; i < parallelism*10; i++ {
			_ = q.Add(i) // add is blocking call, so we can't write more parallelism in a same time
		}
	}()
	time.Sleep(time.Second)
	mu.Lock()
	if int32(parallelism) != inflight.Load() {
		t.Fail()
	}
	mu.Unlock()
	// but result still pending, we wait for wait groups
	if len(res) != 0 {
		t.Fail()
	}
	// done wait group in reverse order
	for _, wg := range wgMap {
		wg.Done()
	}
	mu.Lock()
	for i, data := range res {
		if i != data {
			t.Fail() // order should be same
		}
	}
	mu.Unlock()
	q.Close()
}

func TestGracefullyShutdown(t *testing.T) {
	var res []int
	mu := sync.Mutex{}
	q := New[int, any](context.Background(), 5, func(i int, a any) chan error {
		resCh := make(chan error, 1)
		resCh <- nil
		time.Sleep(2 * time.Millisecond)
		return resCh
	}, func(data int) any {
		time.Sleep(time.Millisecond)
		return nil
	}, func(data int, _ any, _ time.Time, _ error) {
		mu.Lock()
		defer mu.Unlock()
		res = append(res, data)
	})
	go func() {
		iter := 0
		for {
			if err := q.Add(iter); err != nil {
				return
			}
			iter++
			time.Sleep(10 * time.Millisecond)
		}
	}()
	time.Sleep(time.Second)
	q.Close()
}

func TestRandomParseDelay(t *testing.T) {
	parallelism := 10
	inputEventsCount := 100

	mu := sync.Mutex{}
	counter := 0
	validateCounter := func(counter int) {
		if counter > parallelism {
			t.Fail()
		}
	}
	ackIter := 0
	pushIter := 0
	q := New[int, int](context.Background(), parallelism, func(i int, a int) chan error {
		resCh := make(chan error, 1)
		resCh <- nil
		if pushIter != a {
			t.Fail()
		}
		mu.Lock()
		defer mu.Unlock()
		pushIter++
		return resCh
	}, func(data int) int {
		mu.Lock()
		fmt.Printf("%d STARTED, counter:%d->%d\n", data, counter, counter+1)
		counter++
		validateCounter(counter)
		mu.Unlock()

		time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)

		mu.Lock()
		fmt.Printf("%d FINISHED, counter:%d->%d\n", data, counter, counter-1)
		validateCounter(counter)
		counter--
		mu.Unlock()
		return data
	}, func(data int, _ int, _ time.Time, _ error) {
		mu.Lock()
		defer mu.Unlock()
		if ackIter != data {
			t.Fail()
		}
		ackIter++
	})
	for i := 0; i < inputEventsCount; i++ {
		if err := q.Add(i); err != nil {
			t.Fail()
		}
	}
	for {
		mu.Lock()
		if ackIter == inputEventsCount && pushIter == inputEventsCount {
			mu.Unlock()
			break
		} else {
			time.Sleep(time.Millisecond)
		}
		mu.Unlock()
	}
	q.Close()
}
