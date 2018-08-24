package conn_pool

import (
	"fmt"
	"sync"
	"time"
)

var (
	ErrClosed   = errors.New("connection already closed")
	ErrTimeOut  = errors.New("connection time out")
	ErrBusy     = errors.New("no available connection to use")
	ErrNewError = errors.New("create connection error")
)

type ConnObj struct {
	MainElem   interface{}
	IsClosed   bool
	TimeOut    time.Duration
	CreateTime time.Duration
}

//connection is available or not available
func (co *ConnObj) is_conn_available() error {

}

type Pool struct {
	cap        int
	len        int
	mu         sync.Mutex
	factory    func(interface{}, error) *ConnObj
	destructor func(interface{}, error)
	objs       chan *ConnObj
}

func NewPool(cap int, factory func(interface{}, error) *ConnObj, destructor func(interface{}, error)) {
	pool := &Pool{
		cap:        cap,
		len:        0,
		factory:    factory,
		destructor: destructor,
		objs:       make(chan *ConnObj, cap),
	}
	return pool
}

func (pool *Pool) get() (*ConnObj, error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	select {
	case obj := <-pool.objs:
		if obj == nil || obj.IsClosed() {
			pool.len--
			return nil, ErrClosed
		} else if obj.TimeOut {
			pool.len--
			return nil, ErrTimeOut
		}
		return obj, nil
	default:
		if pool.len < pool.cap {
			if new_obj, err = pool.factory(); err != nil {
				pool.len++
				return new_obj, nil
			} else {
				return nil, ErrNewError
			}
		} else {
			return nil, ErrBusy
		}
	}
	return nil, nil
}

func (pool *Pool) put(obj *ConnObj) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	select {
	case pool.objs <- obj:
	default:
	}
}
