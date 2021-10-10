package runner

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

type OnPanicRecoverFunc func(task string, err interface{}, stackinfo string)

var pcb OnPanicRecoverFunc = func(task string, err interface{}, stackinfo string) {
	log.Printf("[PANIC] err:%v, name:%s, data:%s", err, task, stackinfo)
}

func RegistPanicRecoverFunc(cb OnPanicRecoverFunc) {
	pcb = cb
}

type Runner struct {
	threads int
	tasks   map[string]Task
	cost    time.Duration
	errs    map[string]error
}

type Task func(ctx context.Context) error

func New(threads int) *Runner {
	return &Runner{
		threads: threads,
		tasks:   make(map[string]Task),
		errs:    make(map[string]error),
	}
}

func (r *Runner) Add(name string, tk Task) *Runner {
	r.tasks[name] = tk
	return r
}

func (r *Runner) doTask(ctx context.Context, name string, task Task) (err error) {
	defer func() {
		if e := recover(); e != nil {
			stack := string(debug.Stack())
			err = fmt.Errorf("panic:%v, name:%s, stack:%s", e, name, stack)
			pcb(name, e, stack)
		}
	}()
	return task(ctx)
}

func (r *Runner) ErrInfo() map[string]error {
	return r.errs
}

func (r *Runner) Run(ctx context.Context) error {
	begin := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(len(r.tasks))
	batch := semaphore.NewWeighted(int64(r.threads))
	lck := sync.Mutex{}
	var onErr error
	for name := range r.tasks {
		name := name
		task := r.tasks[name]
		if err := batch.Acquire(ctx, 1); err != nil {
			return err
		}
		go func() {
			defer func() {
				batch.Release(1)
				wg.Done()
			}()
			err := r.doTask(ctx, name, task)
			if err != nil {
				lck.Lock()
				r.errs[name] = err
				onErr = err
				lck.Unlock()
			}
		}()
	}
	wg.Wait()
	r.cost = time.Since(begin)
	if onErr != nil {
		return onErr
	}
	return nil
}

func (r *Runner) Cost() time.Duration {
	return r.cost
}
