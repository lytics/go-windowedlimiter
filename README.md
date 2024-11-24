# go-slidingwindow

An implementation of a fully-featured sliding window rate limiter in golang.

Currently extremely pre-alpha. The API is guaranteed to change, as are the internals.

## Goals

- [x] Sliding window algorithm for simple settings and automatic smoothing past an initial burst
- [ ] Optimized for distributed systems with an arbitrary number of processes/goroutines fighting over available rate
- [ ] Simple Redis commands both for redis CPU reasons, and for ease of porting to other backends
- [ ] Comprehensive testing/benchmarking suite to verify not only that the limiter is basically working, but also that it is doing what it's supposed to on a fine time granularity, in a relatively distributed use-case
- [ ] Explicitly tested for correctness and performance
  - [ ] Low volume (cpu usage/latency of a single request)
    - Default to asynchronous increments
    - Support synchronous for hard-capped usecases
  - [ ] High volume (max_rate-1)
    - [ ] Support batching increments if we're in async mode?
  - [ ] Max volume (max_rate)
    - [x] Use a mitigation cache to efficiently block requests
    - [ ] Use a single goroutine to control the mitigation cache per-process
    - [ ] Load balance the allowed rate across all waiting threads
    - [ ] Have the mitigation cache coordinate cross-process such that all processes/threads only try at the allowed rate
- [x] Configurable open or closed failure modes (default to open)

## Design

### High-level concepts

- [CloudFlare writeup](https://blog.cloudflare.com/counting-things-a-lot-of-different-things/).
- [Mechanical sympathy](https://martinfowler.com/articles/lmax.html?ref=wellarchitected#QueuesAndTheirLackOfMechanicalSympathy)

### Use-Cases

- I am an distributed HTTP service accepting client requests and want to rate limit in a way where I can return a 429
  - No control over request rate at the edge
- I am a distributed service that sometimes needs to do a thing that needs to be rate limited, but not on any known cadence
- I am a distributed service that is a tight loop on doing a thing that needs to be rate limited
- I am multiple of any or all of the above at the same time

### Current API

The initial API has been mindlessly copied from other implementations, but is probably not ideal from a humane golang point of view.

- String key for global coordination
  - This seems ideal given we have to store the key externally
- An object for each rate limit, but with the expectation that clients may not actually keep it, so a global registry of objects
  - This is pretty non-ideal both from an efficiency point of view, and from a complexity point of view
  - What if you want to dynamically change the rate limit for a key?
  - What if two different codepaths are using the same key but different rate settings? Maybe the correct thing to do is have each codepath use the same key, but separate limiter settings so the lower-limit one gets completely stopped if the higher-limit one is using more than the lower-limit one's max rate? Could be useful for work sharing systems
- Async `Allow(context.Context) bool` method for when you need to return an error to a client
  - This sucks but is probably necessary for usecases where an external client is authoritative on the cadence of attempts
- `Wait(context.Context) error` method for when you just want to block until you're able to do your operation

### Possible API

- Maybe move to just a global registry, with raw functions? Having a registry of objects seems maybe pointless and is just mixing instantiation of a key with referencing it
- `Allow(context.Context) bool` is really not preferable as poorly thought out clients will call it in a tight loop, which is hard to optimize lib-side, but also pretty required given the HTTP use-case
- `Wait(context.Context) error` is ok as far as optimizing lib-side, but in the event that a client is looping over it, it doesn't have the ability to do efficient persistence (e.g. for an evenly load balanced group of waiters).
- `WaitAtMost(context.Context, time.Duration) error` might be convenient in cases where you know you're not in control of incoming requests but want to inject delay as flow control
- In the event that the client knows it wants to do something repeatedly, a channel is probably more humane, as it's persistent and clean to wait on
