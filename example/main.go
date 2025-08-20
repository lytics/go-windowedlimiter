package main

import (
	"context"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/lytics/go-windowedlimiter"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type rateKey struct {
	namespace string
	id        string
}

func (r rateKey) String() string {
	return r.namespace + "-" + r.id
}

func main() {
	zl, err := zap.NewDevelopment()
	if err != nil {
		panic("an error was not expected when creating zap logger: " + err.Error())
	}

	mr, err := miniredis.Run()
	if err != nil {
		panic("an error was not expected when opening a stub redis connection: " + err.Error())
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	keyConfFn := func(ctx context.Context, key rateKey) *windowedlimiter.KeyConf {
		// Normally you'd hit a database or use a global default
		return &windowedlimiter.KeyConf{
			Rate:     1, // 10 requests per second
			Interval: 10 * time.Second,
		}
	}

	rl := windowedlimiter.New[rateKey](
		context.Background(),
		rdb,
		keyConfFn,
		windowedlimiter.OptionWithLogger[rateKey](zl),
	)

	waitReqs := 10
	startWait := time.Now()
	for range waitReqs {
		rl.Wait(context.Background(), rateKey{namespace: "some_expensive_operation", id: "bob"})
		time.Sleep(500 * time.Millisecond)
	}
	endWait := time.Now()

	startAllow := time.Now()
	allowed := 0
	for range 100 {
		if rl.Allow(context.Background(), rateKey{namespace: "some_other_operation", id: "bob"}) {
			allowed++
		}
		time.Sleep(5 * time.Second)
	}
	endAllow := time.Now()

	zl.Info("Average Rate",
		zap.String("type", "Wait()"),
		zap.Float64("rate", float64(waitReqs)/float64(endWait.Sub(startWait).Seconds())),
	)
	zl.Info("Average Rate",
		zap.String("type", "Allow()"),
		zap.Float64("rate", float64(allowed)/float64(endAllow.Sub(startAllow).Seconds())),
		zap.Int("allowed", allowed),
	)

}
