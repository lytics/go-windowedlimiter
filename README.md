# go-slidingwindow

An implementation of a redis-backed sliding window rate limiter in Go, initally based on the [CloudFlare writeup](https://blog.cloudflare.com/counting-things-a-lot-of-different-things/).

Currently extremely pre-alpha. The API is guaranteed to change, as are the internals.

## Goals

- [x] Sliding window algorithm for simple settings and automatic smoothing past an initial burst
- [ ] Optimized for distributed systems with an arbitrary number of processes/goroutines fighting over available rate
- [ ] Simple Redis commands (not complicated lua) for Redis CPU usage reasons
- [ ] In memory mitigation caching
- [ ] Comprehensive testing/benchmarking suite to verify not only that the limiter is basically working, but also that it is doing what it's supposed to on a fine time granularity, in a relatively distributed use-case
- [x] Configurable open or closed failure modes (default to open)
- [ ] Async `Allow()` function for when you need to return an error to a client
- [ ] First come, first served `Wait()` function for when you just want to block until you're able to do your operation
