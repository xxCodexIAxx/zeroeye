package analytics

import (
	"context"
	"sync"
	"testing"
	"time"
)

func waitForCollectorStopped(t *testing.T, c *Collector) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		c.mu.RLock()
		running := c.running
		stopping := c.stopping
		c.mu.RUnlock()
		if !running && !stopping {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("collector did not stop")
}

func collectorState(c *Collector) (running bool, stopping bool, stopCh chan struct{}) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.running, c.stopping, c.stopCh
}

func TestCollectorStartIsIdempotent(t *testing.T) {
	c := NewCollector()
	c.flushInterval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.Start(ctx)
	running, stopping, firstStopCh := collectorState(c)
	if !running || stopping {
		t.Fatalf("collector should be running after first Start, running=%v stopping=%v", running, stopping)
	}

	for i := 0; i < 10; i++ {
		c.Start(ctx)
	}
	running, stopping, secondStopCh := collectorState(c)
	if !running || stopping {
		t.Fatalf("collector should remain running after repeated Start, running=%v stopping=%v", running, stopping)
	}
	if firstStopCh != secondStopCh {
		t.Fatalf("repeated Start replaced active stop channel")
	}

	c.Stop()
	waitForCollectorStopped(t, c)
}

func TestCollectorConcurrentStartIsSafe(t *testing.T) {
	c := NewCollector()
	c.flushInterval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const callers = 64
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			c.Start(ctx)
		}()
	}
	wg.Wait()

	running, stopping, stopCh := collectorState(c)
	if !running || stopping || stopCh == nil {
		t.Fatalf("collector should have exactly one active run after concurrent Start, running=%v stopping=%v stopCh nil=%v", running, stopping, stopCh == nil)
	}

	c.Stop()
	waitForCollectorStopped(t, c)
}

func TestCollectorStopIsNonBlockingAndIdempotent(t *testing.T) {
	c := NewCollector()
	c.flushInterval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.Start(ctx)
	c.Stop()
	c.Stop()

	waitForCollectorStopped(t, c)
}

func TestCollectorCanRestartAfterStop(t *testing.T) {
	c := NewCollector()
	c.flushInterval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.Start(ctx)
	_, _, firstStopCh := collectorState(c)
	c.Stop()
	waitForCollectorStopped(t, c)

	c.Start(ctx)
	running, stopping, secondStopCh := collectorState(c)
	if !running || stopping {
		t.Fatalf("collector should be running after restart, running=%v stopping=%v", running, stopping)
	}
	if firstStopCh == secondStopCh {
		t.Fatalf("restart should create a fresh stop channel")
	}

	c.Stop()
	waitForCollectorStopped(t, c)
}
