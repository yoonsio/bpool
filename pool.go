package bpool

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

const (
	DefaultBufferSize = 100
)

var (
	ErrInvalidPoolSize    = errors.New("pool size must be larger than 0")
	ErrInvalidBufferSize  = errors.New("buffer size must be larger than 0")
	ErrInvalidBatchSize   = errors.New("batch size cannot be 0 or bigger than buffer length")
	ErrChannelClosed      = errors.New("job channel closed")
	ErrChannelUnavailable = errors.New("result channel unavailable")
)

// Pool implements goroutine worker pool that accepts batch jobs
type Pool struct {
	size    int
	bufsize int
	eg      *errgroup.Group
	cancel  context.CancelFunc
	ch      chan batchJob
	rm      *sync.Map
}

// NewPool returns a new BatchPool from given options
func NewPool(opts ...PoolOption) (*Pool, error) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{
		size:    runtime.NumCPU(),
		bufsize: DefaultBufferSize,
		eg:      &errgroup.Group{},
		cancel:  cancel,
		rm:      &sync.Map{},
	}
	for _, opt := range opts {
		opt(p)
	}
	if p.size <= 0 {
		return nil, ErrInvalidPoolSize
	}
	if p.bufsize <= 0 {
		return nil, ErrInvalidBufferSize
	}
	p.ch = make(chan batchJob, p.bufsize)
	for i := 0; i < p.size; i++ {
		p.eg.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case job, ok := <-p.ch:
					if !ok {
						return ErrChannelClosed
					}
					v, err := job.Do(ctx)
					ch, ok := p.rm.Load(job.bid)
					if !ok {
						log.Printf("WARN: dropping job with invalid bid: %s", job.bid)
						continue
					}
					ch.(chan Result) <- Result{Val: v, Err: err}
				}
			}
		})
	}
	return p, nil
}

// Limit returns number of worker goroutines in the pool
func (p *Pool) Size() int {
	return p.size
}

// BufferSize returns size of job buffer
func (p *Pool) BufferSize() int {
	return p.bufsize
}

// Close cancels worker context and waits until all worker goroutines are terminated
// This method is idempotent and can be called multiple times
func (p *Pool) Close() error {
	p.cancel()
	return p.eg.Wait()
}

// Do dispatches batch jobs to available workers and waits until all jobs are processed
// This method does not return an error even if one or more jobs returns an error
func (p *Pool) Do(ctx context.Context, jobs []Job) (*BatchResult, error) {
	if len(jobs) == 0 || len(jobs) > p.bufsize {
		return nil, ErrInvalidBatchSize
	}
	uid, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("failed to generate uuid: %w", err)
	}
	bid := uid.String()
	if _, ok := p.rm.Load(bid); ok {
		return nil, fmt.Errorf("bid conflict: '%s'", bid)
	}
	p.rm.Store(bid, make(chan Result, len(jobs)))
	for _, job := range jobs {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case p.ch <- batchJob{Job: job, bid: bid}:
		}
	}
	ch, ok := p.rm.Load(bid)
	if !ok {
		return nil, ErrChannelUnavailable
	}
	rch := ch.(chan Result)
	res := &BatchResult{Results: make([]Result, len(jobs))}
	for i := range jobs {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case r, ok := <-rch:
			if !ok {
				return nil, ErrChannelClosed
			}
			res.Results[i] = r
			if r.Err != nil {
				res.hasError = true
			}
		}
	}
	close(rch)
	p.rm.Delete(bid)
	return res, nil
}

// PoolOption represents optional configuration for BatchPool
type PoolOption func(*Pool)

// WithSize sets number of active goroutines in a pool
func WithSize(size int) PoolOption {
	return func(p *Pool) {
		p.size = size
	}
}

// WithBufferSize sets buffered channel size for batch jobs
func WithBufferSize(bufsize int) PoolOption {
	return func(p *Pool) {
		p.bufsize = bufsize
	}
}
