package slidingwindow

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// setupBench is a simplified version of setup for benchmarks
func setupBench(b *testing.B, ctx context.Context, rate int64, interval time.Duration) (*Limiter, string) {
	b.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		b.Fatalf("an error '%s' was not expected when opening a stub redis connection", err)
	}
	b.Cleanup(mr.Close)

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	keyConfFn := func(ctx context.Context, key string) *KeyConf {
		return &KeyConf{Rate: rate, Interval: interval}
	}
	// Benchmarks shouldn't log to avoid skewing results.
	l := New(ctx, rdb, keyConfFn)
	key := fmt.Sprintf("%s-%d", b.Name(), time.Now().UnixNano()%1000)
	b.Cleanup(func() {
		l.Close()
		_ = rdb.Close()
	})
	return l, key
}

func setup(t *testing.T, ctx context.Context, rate int64, interval time.Duration) (*miniredis.Miniredis, *Limiter, string) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	key := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano()%1000)
	keyConfFn := func(ctx context.Context, key string) *KeyConf {
		return &KeyConf{Rate: rate, Interval: interval}
	}
	l := New(ctx, rdb, keyConfFn, Options{Logger: NewLogger(t), BatchDuration: 1 * time.Microsecond})
	t.Cleanup(func() {
		l.Close()
		_ = rdb.Close()
	})
	// avoid testing over the first interval wrap, which can cause more requests to
	// be allowed
	time.Sleep(time.Until(time.Now().Truncate(interval).Add(interval)))
	return mr, l, key
}

func analyzeIntervals(t *testing.T, logger *zap.Logger, _ time.Duration, granularity time.Duration, rate int64, intervals map[time.Time]int64) {
	t.Helper()
	logger = logger.WithOptions(zap.AddCallerSkip(1))
	var minInterval time.Time
	var maxInterval time.Time
	for s := range intervals {
		if minInterval.IsZero() || s.Before(minInterval) {
			minInterval = s
		}
		if maxInterval.IsZero() || s.After(maxInterval) {
			maxInterval = s
		}
	}
	for i := minInterval; i.Before(maxInterval); i = i.Add(granularity) {
		logger.Sugar().Infof("requests in interval %v: %d", i.Format("05.000"), intervals[i])
	}
	assert.GreaterOrEqual(t, intervals[minInterval.Add(granularity*0)], rate, "first interval should have at least rate requests")
	assert.Equal(t, int64(0), intervals[minInterval.Add(granularity*1)], "second interval should have zero requests")
}

type logger interface {
	Log(args ...any)
}

type testOutput struct {
	logger
}

func (o *testOutput) Write(p []byte) (n int, err error) {
	// The escape sequence clears the line before writing the log message.
	// This is unfortunately necessary as we can't call t.Helper() from all of
	// zap's logging functions, which means t.Log will include an incorrect
	// caller
	line := "\x1b[2K\r" + strings.TrimSpace(string(p))
	o.Log(line)
	return len(line), nil
}

type writeSyncer struct {
	io.Writer
}

func (w writeSyncer) Sync() error {
	return nil
}

func NewLogger(t logger) *zap.Logger {
	config := zap.NewDevelopmentEncoderConfig()
	// config.TimeKey = ""
	return zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(config),
		writeSyncer{&testOutput{t}},
		zap.InfoLevel,
	), zap.AddCaller())
}
