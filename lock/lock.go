// Package lock A distributed lock implementation that based on a single redis node.
// The algorithm can see https://redis.io/topics/distlock.
package lock

import (
	"errors"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

var (
	ErrLockNotAcquired  = errors.New("lock not acquired")
	ErrUnlockFailed     = errors.New("unlock failed")
	ErrUnlockKeyExpired = errors.New("unlock key expired")
	UnlockLuaScript     = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
)

// A DistributedMutex is distributed lock that based on single redis node.
type DistributedMutex struct {
	// A key that the redis client will set.
	Key string

	// A value that the key will be related. The value may be a request id.
	Value string

	// A redis client.
	Client *redis.Client

	// A option of the lock.
	Opt *Options

	// A local lock.
	Mutex sync.Mutex
}

// Options defines some options for the lock.
type Options struct {
	// The maximum expired duration of the key.
	// It also indicates the timeout to obtain a lock.
	// Default: 5s
	Expiration time.Duration

	// The count of a lock will be retired. When it is zero, it indicates the lock will not be retired.
	// Default: 0
	RetryCount int

	// The duration of the next time to lock.
	// Default: 100ms
	RetryDelay time.Duration
}

// NewDistributedMutex creates a distributed mutex.
func NewDistributedMutex(key, value string, client *redis.Client, opt *Options) *DistributedMutex {
	opt.init()
	return &DistributedMutex{
		Key:    key,
		Value:  value,
		Client: client,
		Opt:    opt,
	}
}

// Lock try to acquire a lock.
func (d *DistributedMutex) Lock() error {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	retryCount := d.Opt.RetryCount
	for {
		ok, err := d.tryLock()
		if err != nil {
			return err
		} else if ok {
			return nil
		}
		if retryCount <= 0 {
			return ErrLockNotAcquired
		}
		time.Sleep(d.Opt.RetryDelay)
		retryCount--
	}
}

func (d *DistributedMutex) tryLock() (bool, error) {
	ok, err := d.Client.SetNX(d.Key, d.Value, d.Opt.Expiration).Result()
	return ok, err
}

// Unlock release the lock.
func (d *DistributedMutex) Unlock() error {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	res, err := d.Client.Eval(UnlockLuaScript, []string{d.Key}, d.Value).Result()
	if err == redis.Nil {
		return ErrUnlockKeyExpired
	}
	if err != nil {
		return err
	}
	if n, ok := res.(int64); ok && n == 1 {
		return nil
	}
	return ErrUnlockFailed
}

func (o *Options) init() *Options {
	if o.Expiration < 1 {
		o.Expiration = 5 * time.Second
	}
	if o.RetryCount < 0 {
		o.RetryCount = 0
	}
	if o.RetryDelay < 1 {
		o.RetryDelay = 100 * time.Millisecond
	}
	return o
}
