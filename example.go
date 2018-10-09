package main

import (
    "github.com/ckanner/redis-lock/lock"
    "github.com/go-redis/redis"
    "log"
)

func getRedisClient() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		DB:       0,
		PoolSize: 100,
	})
	return client, nil
}

func logic()  {
    // do some logic
}

func main() {
    client, err := getRedisClient()
    if err != nil {
        log.Fatalf("get a redis client fail, %s", err)
    }
	distMutex := lock.NewDistributedMutex("key", "request-id", client, &lock.Options{})
	err = distMutex.Lock()
	if err != nil {
	    log.Fatalf("get a lock fail, %s", err)
    }
	defer distMutex.Unlock()
	logic()
}

