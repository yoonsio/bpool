package bpool_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/yoonsio/bpool"
	"golang.org/x/sync/errgroup"
)

const (
	maxJobs = 100
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// TestBatch processes single batch request with default config
func TestBatch(t *testing.T) {
	type testCase struct {
		name    string
		numJobs int
		size    int
		bufsize int
	}
	testCases := []testCase{
		{
			name:    "single job",
			numJobs: 1,
		},
		{
			name:    "more workers than jobs",
			size:    10,
			numJobs: 1,
		},
		{
			name:    "more jobs than number of workers",
			size:    1,
			numJobs: 10,
		},
		{
			name:    "buffer size smaller than number of workers",
			size:    10,
			bufsize: 1,
			numJobs: 1,
		},
	}
	for i := 0; i < 5; i++ {
		numJobs := rand.Intn(maxJobs) + 1
		testCases = append(testCases, testCase{
			name:    fmt.Sprintf("%d jobs", numJobs),
			numJobs: numJobs,
		})
	}
	jobs := generateTestJobs(maxJobs, 0, nil, false)
	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			testJobs := jobs[:tc.numJobs]
			opts := []bpool.PoolOption{}
			if tc.size != 0 {
				opts = append(opts, bpool.WithSize(tc.size))
			}
			if tc.bufsize != 0 {
				opts = append(opts, bpool.WithBufferSize(tc.bufsize))
			}
			ctx := context.Background()
			bp, err := bpool.NewPool(opts...)
			if err != nil {
				tt.Errorf("failed to initialize pool: %+v", err)
				return
			}
			defer bp.Close()
			bres, err := bp.Do(ctx, testJobs)
			if err != nil {
				tt.Errorf("failed to process batch job: %v", err)
				return
			}
			if bres.HasError() {
				tt.Errorf("found unexpected request processing error")
				return
			}
			if len(testJobs) != len(bres.Results) {
				tt.Errorf("number of jobs '%d' is different from number of results '%d'", len(testJobs), len(bres.Results))
				return
			}
			processedRequestIDs := make([]int, tc.numJobs)
			for i, resp := range bres.Results {
				processedRequestIDs[i] = resp.Val.(int)
			}
			sort.Ints(processedRequestIDs)
			for i := 0; i < tc.numJobs; i++ {
				if processedRequestIDs[i] != i {
					tt.Errorf("unexpected response found")
					return
				}
			}
		})
	}
}

// TestBatchParallel processes multiple batch requests in parallel with default config
func TestBatchParallel(t *testing.T) {
	type testCase struct {
		name       string
		numBatches int
	}
	testCases := []testCase{}
	for i := 0; i < 5; i++ {
		numBatches := rand.Intn(10) + 2
		testCases = append(testCases, testCase{
			name:       fmt.Sprintf("%d batches", numBatches),
			numBatches: numBatches,
		})
	}
	jobs := generateTestJobs(maxJobs, 0, nil, false)
	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			ctx := context.Background()
			bp, err := bpool.NewPool()
			if err != nil {
				tt.Errorf("failed to initialize pool: %+v", err)
				return
			}
			defer bp.Close()
			eg := &errgroup.Group{}
			for i := 0; i < tc.numBatches; i++ {
				numRequests := rand.Intn(maxJobs) + 1
				testJobs := jobs[:numRequests]
				eg.Go(func() error {
					bres, err := bp.Do(ctx, testJobs)
					if err != nil {
						return fmt.Errorf("failed to dispatch requests: %w", err)
					}
					if bres.HasError() {
						return fmt.Errorf("found unexpected request processing error")
					}
					if len(testJobs) != len(bres.Results) {
						return fmt.Errorf("number of jobs '%d' is different from number of results '%d'", len(testJobs), len(bres.Results))
					}
					processedRequestIDs := make([]int, numRequests)
					for i, resp := range bres.Results {
						processedRequestIDs[i] = resp.Val.(int)
					}
					sort.Ints(processedRequestIDs)
					for i := 0; i < numRequests; i++ {
						if processedRequestIDs[i] != i {
							return fmt.Errorf("unexpected response found")
						}
					}
					return nil
				})
			}
			if err := eg.Wait(); err != nil {
				tt.Errorf("%+v", err)
			}
		})
	}
}

// TestPoolError tests error handling for the pool
func TestPoolError(t *testing.T) {
	testCases := []struct {
		name        string
		jobs        []bpool.Job
		bufsize     int
		expectedErr error
	}{
		{
			name:        "zero jobs",
			jobs:        make([]bpool.Job, 0),
			bufsize:     1,
			expectedErr: bpool.ErrInvalidBatchSize,
		},
		{
			name:        "null jobs",
			jobs:        nil,
			bufsize:     1,
			expectedErr: bpool.ErrInvalidBatchSize,
		},
		{
			name:        "buffer size smaller than number of jobs",
			jobs:        make([]bpool.Job, 5),
			bufsize:     1,
			expectedErr: bpool.ErrInvalidBatchSize,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			ctx := context.Background()
			bp, err := bpool.NewPool(bpool.WithBufferSize(tc.bufsize))
			if err != nil {
				tt.Errorf("failed to initialize pool: %+v", err)
				return
			}
			defer bp.Close()
			bresp, err := bp.Do(ctx, tc.jobs)
			if tc.expectedErr != err {
				tt.Errorf("expected error '%v' but found '%v'", tc.expectedErr, err)
				return
			}
			if bresp != nil {
				tt.Errorf("expected empty batch response but found '%+v'", bresp)
			}
		})
	}
}

// TestConfig tests pool configuration
func TestConfig(t *testing.T) {
	testCases := []struct {
		name        string
		size        int
		bufsize     int
		expectedErr error
	}{
		{
			name:        "default config",
			size:        4, // using fixed value instead of runtime.NumCPU() to avoid flaky tests
			bufsize:     bpool.DefaultBufferSize,
			expectedErr: nil,
		},
		{
			name:        "buffer size smaller than pool size",
			size:        4,
			bufsize:     1,
			expectedErr: nil,
		},
		{
			name:        "zero pool size",
			size:        0,
			bufsize:     bpool.DefaultBufferSize,
			expectedErr: bpool.ErrInvalidPoolSize,
		},
		{
			name:        "negative pool size",
			size:        -3,
			bufsize:     bpool.DefaultBufferSize,
			expectedErr: bpool.ErrInvalidPoolSize,
		},
		{
			name:        "zero buffer length",
			size:        1,
			bufsize:     0,
			expectedErr: bpool.ErrInvalidBufferSize,
		},
		{
			name:        "negative buffer size",
			size:        1,
			bufsize:     -5,
			expectedErr: bpool.ErrInvalidBufferSize,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			bp, err := bpool.NewPool(
				bpool.WithBufferSize(tc.bufsize),
				bpool.WithSize(tc.size),
			)
			if bp != nil {
				if bp.Size() != tc.size {
					tt.Errorf("expected pool size to be '%d' but found '%d'", tc.size, bp.Size())
					return
				}
				if bp.BufferSize() != tc.bufsize {
					tt.Errorf("expected buffer size to be '%d' but found '%d'", tc.bufsize, bp.BufferSize())
					return
				}
			}
			if !errors.Is(err, tc.expectedErr) {
				tt.Errorf("expected err '%v' but found '%v'", tc.expectedErr, err)
				return
			}
		})
	}
}

// TestContextError tests context error handling within worker pool
func TestContextError(t *testing.T) {
	type testCase struct {
		name        string
		ctx         context.Context
		cancel      context.CancelFunc
		expectedErr error
	}
	testCases := []testCase{
		{
			name: "normal context",
			ctx:  context.Background(),
		},
	}

	cpCtx, cpCancelFunc := context.WithCancel(context.Background())
	testCases = append(testCases, testCase{
		name:        "context cancellled",
		ctx:         cpCtx,
		cancel:      cpCancelFunc,
		expectedErr: context.Canceled,
	})

	toCtx, toCancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer toCancel()
	testCases = append(testCases, testCase{
		name:        "context timeout",
		ctx:         toCtx,
		expectedErr: context.DeadlineExceeded,
	})

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			bp, err := bpool.NewPool()
			if err != nil {
				tt.Errorf("failed to initialize batch pool: %v", err)
				return
			}
			defer bp.Close()
			jobs := generateTestJobs(10, 0, nil, false)
			if tc.cancel != nil {
				tc.cancel()
			}
			if _, err := bp.Do(tc.ctx, jobs); err != tc.expectedErr {
				tt.Errorf("expected error '%v' but found '%v'", tc.expectedErr, err)
				return
			}
			// explicitly call Close() multiple times
			for i := 0; i < 1; i++ {
				if err := bp.Close(); err != context.Canceled {
					tt.Errorf("expected error '%v' but found '%v'", context.Canceled, err)
					return
				}
			}
		})
	}
}

// TestJobError tests job error handling
func TestJobError(t *testing.T) {
	testCases := []struct {
		name       string
		jobErr     error
		partialErr bool
	}{
		{
			name: "no process errors",
		},
		{
			name:   "process error",
			jobErr: errors.New("test"),
		},
		{
			name:       "partial process error",
			jobErr:     errors.New("test"),
			partialErr: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			numRequests := rand.Intn(maxJobs) + 1
			jobs := generateTestJobs(numRequests, 0, tc.jobErr, tc.partialErr)
			testJobs := jobs[:numRequests]
			bp, err := bpool.NewPool()
			if err != nil {
				tt.Errorf("failed to initialize pool: %+v", err)
				return
			}
			defer bp.Close()
			ctx := context.Background()
			bresp, err := bp.Do(ctx, testJobs)
			if err != nil {
				tt.Errorf("failed to dispatch requests: %+v", err)
				return
			}
			if bresp.HasError() != (tc.jobErr != nil) {
				tt.Errorf("expected batch response hasError to be '%t' but found '%t'", tc.jobErr != nil, bresp.HasError())
				return
			}
			if len(testJobs) != len(bresp.Results) {
				tt.Errorf("request length '%d' is different from response length '%d'", len(testJobs), len(bresp.Results))
				return
			}
			var respErr error
			for _, resp := range bresp.Results {
				if !tc.partialErr && resp.Err != tc.jobErr {
					tt.Errorf("expected job error '%v' but found '%v'", tc.jobErr, resp.Err)
					return
				}
				if resp.Err != nil {
					respErr = resp.Err
				}
			}
			if respErr != tc.jobErr {
				tt.Errorf("expected process error '%v' but found '%v'", tc.jobErr, respErr)
			}
		})
	}
}

type mockJob struct {
	ID    int
	Err   error
	Delay time.Duration
}

func (r mockJob) Do(ctx context.Context) (any, error) {
	select {
	case <-ctx.Done():
		return r.ID, ctx.Err()
	case <-time.After(r.Delay):
		return r.ID, r.Err
	}
}

func generateTestJobs(
	n int,
	delay time.Duration,
	err error,
	partialErr bool,
) []bpool.Job {
	jobs := make([]bpool.Job, n)
	for i := 0; i < n; i++ {
		jobErr := err
		if partialErr && i%2 == 0 {
			jobErr = nil
		}
		reqDelay := delay
		if delay == 0 {
			reqDelay = time.Duration(rand.Intn(100)+10) * time.Millisecond
		}
		jobs[i] = mockJob{
			ID:    i,
			Err:   jobErr,
			Delay: reqDelay,
		}
	}
	return jobs
}
