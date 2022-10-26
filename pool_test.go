package bpool_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/yoonsio/bpool"
)

const (
	maxRequests = 100
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// TestSingleBatchRequest processes single batch request with default config
func TestSingleBatchRequest(t *testing.T) {
	type testCase struct {
		name        string
		numRequests int
		numWorkers  int
		numChanLen  int
	}
	testCases := []testCase{
		{
			name:        "single request",
			numRequests: 1,
		},
		{
			name:        "more workers than requests",
			numWorkers:  10,
			numRequests: 1,
		},
		{
			name:        "more requests than number of workers",
			numWorkers:  1,
			numRequests: 10,
		},
		{
			name:        "channel length smaller than number of workers",
			numWorkers:  10,
			numChanLen:  1,
			numRequests: 1,
		},
	}
	for i := 0; i < 5; i++ {
		numRequests := rand.Intn(maxRequests) + 1
		testCases = append(testCases, testCase{
			name:        fmt.Sprintf("%d requests", numRequests),
			numRequests: numRequests,
		})
	}
	reqs := generateTestRequests(maxRequests, 0, nil, false)
	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			testReqs := reqs[:tc.numRequests]
			opts := []bpool.BatchPoolOption{}
			if tc.numWorkers != 0 {
				opts = append(opts, bpool.WithNumWorkers(tc.numWorkers))
			}
			if tc.numChanLen != 0 {
				opts = append(opts, bpool.WithChannelLength(tc.numChanLen))
			}
			ctx := context.Background()
			bp, err := bpool.NewBatchPool(opts...)
			if err != nil {
				tt.Errorf("failed to initialize batch pool: %+v", err)
				return
			}
			defer bp.Close()
			bresp, err := bp.Dispatch(ctx, testReqs)
			if err != nil {
				tt.Errorf("failed to dispatch requests: %+v", err)
				return
			}
			if bresp.HasError() {
				tt.Errorf("found unexpected request processing error")
				return
			}
			if len(testReqs) != len(bresp.Responses) {
				tt.Errorf("request length '%d' is different from response length '%d'", len(testReqs), len(bresp.Responses))
				return
			}
			processedRequestIDs := make([]int, tc.numRequests)
			for i, resp := range bresp.Responses {
				processedRequestIDs[i] = resp.Response.(int)
			}
			sort.Ints(processedRequestIDs)
			for i := 0; i < tc.numRequests; i++ {
				if processedRequestIDs[i] != i {
					tt.Errorf("unexpected response found")
					return
				}
			}
		})
	}
}

// TestMultiBatchRequests processes multiple batch requests in parallel with default config
func TestMultiBatchRequests(t *testing.T) {
	type testCase struct {
		name       string
		numBatches int
	}
	testCases := []testCase{}
	for i := 0; i < 5; i++ {
		numBatches := rand.Intn(10) + 1
		testCases = append(testCases, testCase{
			name:       fmt.Sprintf("%d batches", numBatches),
			numBatches: numBatches,
		})
	}
	reqs := generateTestRequests(maxRequests, 0, nil, false)
	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			ctx := context.Background()
			bp, err := bpool.NewBatchPool()
			if err != nil {
				tt.Errorf("failed to initialize batch pool: %+v", err)
				return
			}
			defer bp.Close()
			wg := &sync.WaitGroup{}
			wg.Add(tc.numBatches)
			for i := 0; i < tc.numBatches; i++ {
				numRequests := rand.Intn(maxRequests) + 1
				testReqs := reqs[:numRequests]
				go func() {
					defer wg.Done()
					bresp, err := bp.Dispatch(ctx, testReqs)
					if err != nil {
						tt.Errorf("failed to dispatch requests: %+v", err)
						return
					}
					if bresp.HasError() {
						tt.Errorf("found unexpected request processing error")
						return
					}
					if len(testReqs) != len(bresp.Responses) {
						tt.Errorf("request length '%d' is different from response length '%d'", len(testReqs), len(bresp.Responses))
						return
					}
					processedRequestIDs := make([]int, numRequests)
					for i, resp := range bresp.Responses {
						processedRequestIDs[i] = resp.Response.(int)
					}
					sort.Ints(processedRequestIDs)
					for i := 0; i < numRequests; i++ {
						if processedRequestIDs[i] != i {
							tt.Errorf("unexpected response found")
							return
						}
					}
				}()
			}
			wg.Wait()
		})
	}
}

// TestDispatch tests invalid dispatch usage
func TestDispatchErrorHandling(t *testing.T) {
	testCases := []struct {
		name        string
		requests    []bpool.Request
		chanLen     int
		expectedErr error
	}{
		{
			name:        "zero request size",
			requests:    make([]bpool.Request, 0),
			chanLen:     1,
			expectedErr: bpool.ErrInvalidBatchSize,
		},
		{
			name:        "null request",
			requests:    nil,
			chanLen:     1,
			expectedErr: bpool.ErrInvalidBatchSize,
		},
		{
			name:        "channel length smaller than number of requests",
			requests:    make([]bpool.Request, 5),
			chanLen:     1,
			expectedErr: bpool.ErrInvalidBatchSize,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			ctx := context.Background()
			bp, err := bpool.NewBatchPool(bpool.WithChannelLength(tc.chanLen))
			if err != nil {
				tt.Errorf("failed to initialize batch pool: %+v", err)
				return
			}
			defer bp.Close()
			bresp, err := bp.Dispatch(ctx, tc.requests)
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
		numWorkers  int
		channelLen  int
		expectedErr error
	}{
		{
			name:        "default config",
			numWorkers:  runtime.NumCPU(),
			channelLen:  bpool.DefaultChannelLength,
			expectedErr: nil,
		},
		{
			name:        "channel length smaller than numWorkers",
			numWorkers:  4,
			channelLen:  1,
			expectedErr: nil,
		},
		{
			name:        "zero numWorkers",
			numWorkers:  0,
			channelLen:  bpool.DefaultChannelLength,
			expectedErr: bpool.ErrInvalidConfig,
		},
		{
			name:        "negative numWorkers",
			numWorkers:  -3,
			channelLen:  bpool.DefaultChannelLength,
			expectedErr: bpool.ErrInvalidConfig,
		},
		{
			name:        "zero channel length",
			numWorkers:  1,
			channelLen:  0,
			expectedErr: bpool.ErrInvalidConfig,
		},
		{
			name:        "negative channel length",
			numWorkers:  1,
			channelLen:  -5,
			expectedErr: bpool.ErrInvalidConfig,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			bp, err := bpool.NewBatchPool(
				bpool.WithChannelLength(tc.channelLen),
				bpool.WithNumWorkers(tc.numWorkers),
			)
			if bp != nil {
				if bp.NumWorkers() != tc.numWorkers {
					tt.Errorf("expected numWorkers to be '%d' but found '%d'", tc.numWorkers, bp.NumWorkers())
					return
				}
				if bp.ChannelLength() != tc.channelLen {
					tt.Errorf("expected channel length to be '%d' but found '%d'", tc.channelLen, bp.ChannelLength())
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

// TestContextErrorHandling tests context error handling within worker pool
func TestContextErrorHandling(t *testing.T) {
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
			bp, err := bpool.NewBatchPool()
			if err != nil {
				tt.Errorf("failed to initialize batch pool: %v", err)
				return
			}
			defer bp.Close()
			reqs := generateTestRequests(10, 0, nil, false)
			if tc.cancel != nil {
				tc.cancel()
			}
			if _, err := bp.Dispatch(tc.ctx, reqs); err != tc.expectedErr {
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

// TestProcessErrorHandling tests job error handling
func TestProcessErrorHandling(t *testing.T) {
	testCases := []struct {
		name       string
		processErr error
		partialErr bool
	}{
		{
			name: "no process errors",
		},
		{
			name:       "process error",
			processErr: errors.New("test"),
		},
		{
			name:       "partial process error",
			processErr: errors.New("test"),
			partialErr: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			numRequests := rand.Intn(maxRequests) + 1
			reqs := generateTestRequests(numRequests, 0, tc.processErr, tc.partialErr)
			testReqs := reqs[:numRequests]
			bp, err := bpool.NewBatchPool()
			if err != nil {
				tt.Errorf("failed to initialize batch pool: %+v", err)
				return
			}
			defer bp.Close()
			ctx := context.Background()
			bresp, err := bp.Dispatch(ctx, testReqs)
			if err != nil {
				tt.Errorf("failed to dispatch requests: %+v", err)
				return
			}
			if bresp.HasError() != (tc.processErr != nil) {
				tt.Errorf("expected batch response hasError to be '%t' but found '%t'", tc.processErr != nil, bresp.HasError())
				return
			}
			if len(testReqs) != len(bresp.Responses) {
				tt.Errorf("request length '%d' is different from response length '%d'", len(testReqs), len(bresp.Responses))
				return
			}
			var respErr error
			for _, resp := range bresp.Responses {
				if !tc.partialErr && resp.Err != tc.processErr {
					tt.Errorf("expected process error '%v' but found '%v'", tc.processErr, resp.Err)
					return
				}
				if resp.Err != nil {
					respErr = resp.Err
				}
			}
			if respErr != tc.processErr {
				tt.Errorf("expected process error '%v' but found '%v'", tc.processErr, respErr)
			}
		})
	}
}

type mockRequest struct {
	ID    int
	Err   error
	Delay time.Duration
}

func (r mockRequest) Process(ctx context.Context) (any, error) {
	select {
	case <-ctx.Done():
		return r.ID, ctx.Err()
	case <-time.After(r.Delay):
		return r.ID, r.Err
	}
}

func generateTestRequests(
	n int,
	delay time.Duration,
	err error,
	partialErr bool,
) []bpool.Request {
	reqs := make([]bpool.Request, n)
	for i := 0; i < n; i++ {
		processErr := err
		if partialErr && i%2 == 0 {
			processErr = nil
		}
		reqDelay := delay
		if delay == 0 {
			reqDelay = time.Duration(rand.Intn(100)+10) * time.Millisecond
		}
		reqs[i] = mockRequest{
			ID:    i,
			Err:   processErr,
			Delay: reqDelay,
		}
	}
	return reqs
}
