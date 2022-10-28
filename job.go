package bpool

import (
	"context"
)

// Job provides an interface for jobs that can be submitted to worker pool
type Job interface {
	Do(ctx context.Context) (any, error)
}

type batchJob struct {
	Job
	bid string
}

// Result includes returned value and error from job execution
type Result struct {
	Val any
	Err error
}

// BatchResult includes results for batch jobs
type BatchResult struct {
	hasError bool
	Results  []Result
}

// HasError returns if any of the job results contains error
func (r *BatchResult) HasError() bool {
	return r.hasError
}
