# go-windowedlimiter

An implementation of a fully-featured sliding window rate limiter in golang.

Currently extremely pre-alpha. The API is guaranteed to change, as are the internals.

## Usage

See [example](example/main.go) for basic usage.

## Goals

- [x] Sliding window algorithm for simple settings and automatic smoothing past an initial burst
- [ ] Optimized for distributed systems with an arbitrary number of processes/goroutines fighting over available rate
  - The plan here is to add a registration system such that when a mitigation (see below) is present, all processes will register their number of waiting goroutines, such that each mitigation can track the total global count and divide the hits to the centralized kv store by that number is present, all processes will register their number of waiting goroutines, such that each mitigation can track the total global count and divide the hits to the centralized kv store by that number. Details TBD
- [x] Simple Redis commands both for Redis CPU reasons, and for ease of porting to other backends
- [ ] Comprehensive testing/benchmarking suite to verify not only that the limiter is basically working, but also that it is doing what it's supposed to on a fine time granularity, in a relatively distributed use-case
- [ ] Explicitly tested for correctness and performance
  - [ ] While Redis is degraded
    - [ ] Redis slow
    - [ ] Redis down (fast-fail, e.g. connection refused)
    - [ ] Redis down (slow-fail, e.g. DNS or connection timeout)
  - [ ] Low volume (cpu usage/latency of a single request)
  - [ ] High volume (max_rate-1)
  - [ ] Max volume (max_rate)
    - [x] Use a mitigation cache to efficiently block requests
    - [x] Use a single goroutine per mitigation for local atomicity
    - [x] Load balance the allowed rate across all waiting threads
    - [ ] Have the mitigation cache coordinate cross-process such that all processes/threads only try at the allowed rate

## Design

### High-level concepts

- [CloudFlare writeup](https://blog.cloudflare.com/counting-things-a-lot-of-different-things/).
- [Mechanical sympathy](https://martinfowler.com/articles/lmax.html?ref=wellarchitected#QueuesAndTheirLackOfMechanicalSympathy)

### Use-Cases

- I am an distributed HTTP service accepting client requests and want to rate limit in a way where I can return a 429
  - No control over request rate at the edge, so can't be blocking
- I am a distributed service that sometimes needs to do a thing that needs to be rate limited, but not on any known cadence
  - Non-persistent blocking client
- I am a distributed service that is a tight loop on doing a thing that needs to be rate limited
  - Persistent (looping) blocking client
- I am multiple of any or all of the above at the same time?

### Current API

The initial API has been mindlessly copied from other implementations, but is probably not ideal from a humane golang point of view.

- Generic keys allow arbitrary data to be stored in a retrievable way
  - The key must be both a fmt.Stringer and a comparable type, so that it can be used as a map key and sent to redis as a string
  - This is useful because you can implement whatever namespacing you want in your keys (e.g. namespace, account id, user id, etc.), have the key available to the Key Config Function so that you can use any fields set separately in a type safe way while actually looking up the allowed rates (vs having to parse parts out of an already stringified key)
- A single instance of the rate limiter is expected to be used, with separation determined by the Key type which is passed in
- Async `Allow(context.Context, Key) bool` method for when you need to return an error to a client immediately if the rate limit is exceeded
  - This is for usecases where an external client is authoritative on the cadence of attempts but you don't want blocked goroutines piling up, such as HTTP
- `Wait(context.Context, Key) error` method for when you just want to block until you're able to do your operation
  - This is for usecases like work running systems or queue subscribers where the max number is bounded

### Components

#### Mitigation Cache

`internal/mitigation/` is a thread safe in-memory cache of keys that have exceeded the rate limit. This is in place to ensure that the rate limiter is efficient when a rate limit is exceeded. If a key isn't in the mitigation cache, it is allowed immediately with a `sync.Map` read. Increments are sent via channel to a single global incrementer goroutine, which batches them pipelines them to redis.

For each key in the mitigation cache, a single goroutine will be spawned that will check redis and distribute the allowed rate across all waiting goroutines. The mitigation cache entries get extended while there are active subscribers.

#### FIFO Queue

`internal/fifo/` is a simple FIFO queue used by the mitigation cache to evenly distribute available rate to future Allow calls and waiting goroutines.

#### Rate Limiter

##### TODO

- [ ] TTL for keyConfCache entries?
- [ ] Exponential backoff of retries in wait? If the mitigation cache can actually return real errors in the future
- [ ] Investigate channel-based synchronous API so clients can use `select` to wait for available rate in a more go-like way
