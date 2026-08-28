# Demo transcript

A real run of `make demo` -- contention, an outage, then a check of the
configuration that survived both. Nothing here is hand-edited; regenerate it
with `make demo`.

No LLM API key is involved: all traffic goes to the built-in synthetic provider
fleet.

## A tenant floods the gateway

```console
$ switchyard scenario noisy-neighbour

  noisy neighbour  one tenant floods the gateway; can another keep working?

  Setup
    gateway capacity 12 concurrent requests, weighted fair scheduling
    quiet   offers  1.0 req/s   reserved floor of 6 slots it can always reach
    noisy   offers 25.0 req/s   no floor, so it gets the rest
    the floor is what bounds latency. Weight divides contended capacity, but an in-flight request cannot be preempted, so a tenant without enough reserved slots still waits for one to free.
    size a floor by Little's law -- offered rate x seconds per request. The measured service time is printed below so the sizing can be checked.
    no LLM API key needed: requests go to the built-in synthetic provider
    watch it live from another terminal:
      switchyard top --url http://127.0.0.1:56886 --key sk_sy_admin_6acd2e42773b9c57f308e678a5e61270

  Running
    3.0s     capacity  1/12   quiet:  1 running    0 queued   noisy:  0 running    0 queued
    6.0s     capacity  0/12   quiet:  0 running    0 queued   noisy:  0 running    0 queued
    8.1s  >> noisy tenant starts flooding at 25 req/s (25x the quiet tenant)
    9.1s     capacity  8/12   quiet:  2 running    0 queued   noisy:  6 running   20 queued
   12.1s     capacity 10/12   quiet:  4 running    0 queued   noisy:  6 running   60 queued
   15.1s     capacity 11/12   quiet:  5 running    0 queued   noisy:  6 running   64 queued
   18.1s     capacity 10/12   quiet:  4 running    0 queued   noisy:  6 running   64 queued
   21.1s     capacity  8/12   quiet:  2 running    0 queued   noisy:  6 running   64 queued
   24.1s     capacity  6/12   quiet:  0 running    0 queued   noisy:  6 running   61 queued
   27.1s     capacity  7/12   quiet:  1 running    0 queued   noisy:  6 running   41 queued
   27.9s  >> flood stops
  What happened to the quiet tenant
    slots it needed         2.4   (1 req/s x 2.4s per request), floor is 6
    before the flood       0.75 req/s served   queue wait p95      0ms
    during the flood       0.94 req/s served   queue wait p95      0ms
    the noisy tenant        479 tokens/s served   401 rejected

  Verdict
    PASS  quiet tenant kept being served                  0.94 of 1 req/s offered
    PASS  quiet tenant was not made to wait               queue wait p95 0ms
    PASS  the flood was charged to the tenant causing it  401 noisy rejected, 0 quiet rejected
    PASS  no capacity leaked                              0 in flight, 0 queued at rest

  All guarantees held.
  Re-run with --policy fifo to see this without fair scheduling.
```

## A provider dies mid-traffic

```console
$ switchyard scenario provider-outage

  provider outage  the primary provider starts failing; do clients notice?

  Setup
    steady traffic at 4 req/s through the gateway
    model 'fast' routes to 'fast', falling back to 'slow'
    'fast' will start returning 5xx at 8s and recover at 26s
    no LLM API key needed: both providers are the built-in synthetic fleet
    watch it live from another terminal:
      switchyard top --url http://127.0.0.1:57692 --key sk_sy_admin_9a76add80dc1277f3f3f204b1fb3e948

  Running
    4.0s     fast: closed    5 ok / 0 failed   slow: closed    0 ok / 0 failed
    8.0s     fast: closed    18 ok / 0 failed   slow: closed    0 ok / 0 failed
    8.0s  >> 'fast' starts returning 5xx for every request
   12.1s  >> circuit breaker for 'fast': closed -> open  (retries in 4s)
   12.1s     fast: open      26 ok / 8 failed   slow: closed    1 ok / 0 failed
   16.1s     fast: open      26 ok / 8 failed   slow: closed    4 ok / 0 failed
   20.1s     fast: open      26 ok / 9 failed   slow: closed    10 ok / 0 failed
   24.1s     fast: open      26 ok / 9 failed   slow: closed    13 ok / 0 failed
   26.0s  >> 'fast' recovers
   28.1s     fast: open      26 ok / 10 failed   slow: closed    19 ok / 0 failed
   32.1s     fast: open      26 ok / 10 failed   slow: closed    23 ok / 0 failed
   36.1s     fast: open      26 ok / 10 failed   slow: closed    27 ok / 0 failed
  What the client experienced
    before the outage     28/28 served (100%)
    during the outage     72/72 served (100%)
    after recovery        42/43 served (98%)
    gateway response      10 transparent failovers, 40 requests skipped a provider it knew was down

  Verdict
    PASS  clients kept getting answers during the outage  100% served (no failures)
    PASS  traffic moved to the fallback provider          10 failovers, 10 failures recorded against 'fast'
    PASS  stopped calling the failing provider            40 requests skipped 'fast' while its breaker was open
    PASS  service recovered after the provider did        98% served after recovery
    PASS  no capacity leaked                              0 in flight, 0 queued at rest

  All guarantees held.
```

## Does the configuration hold?

```console
$ switchyard verify

  configuration check  what does switchyard.toml mean, and does it hold?

    your real providers are not called: traffic goes to the built-in synthetic fleet, so this costs nothing and cannot disturb production
    a copy of your configuration runs with test credentials; every limit, weight, floor, budget and route is taken from your file unchanged
  Checking behaviour under load
    2.8s  >> 'globex' floods while 'acme' offers 2.4 req/s -- inside the 6 slots its floor guarantees
    model 'fast' falls back from 'fast' to 'slow'; its breaker needs about 25 failures to trip
   13.3s  >> 'fast' starts failing for every request
   32.1s  >> draining the gateway while requests are still running
  What this configuration means
    capacity          24 concurrent requests: 12 reserved as floors, 12 shared between everyone
    tenant 'acme'     floor 6, ceiling 18, 60% of contended capacity -- its floor alone sustains about 3.4 req/s
                      at the 1.8s per request measured here
      budget          2,000,000 tokens is about 15,625 more requests
                      at the 128 tokens per response measured here
    tenant 'globex'   floor 4, ceiling 12, 20% of contended capacity -- its floor alone sustains about 2.2 req/s
                      at the 1.8s per request measured here
      budget          500,000 tokens is about 3,906 more requests
                      at the 128 tokens per response measured here
    tenant 'initech'  floor 2, ceiling 8, 20% of contended capacity -- its floor alone sustains about 1.1 req/s
                      at the 1.8s per request measured here
      budget          50,000 tokens is about 391 more requests
                      at the 128 tokens per response measured here
    model 'fast'      tries fast, falls back to slow
    model 'slow'      only slow
                      no fallback: a failure here reaches the client
    circuit breaker   opens after about 25 failures within the last 50
                      then waits 5s before probing, doubling on repeat trips
    shutdown          refuses queued work at once and waits up to 30s for running requests

  Verdict
    PASS  capacity is fully allocatable                   12 reserved of 24, 12 shared
    PASS  tenants are isolated from each other            3 of 3 tenants have a reserved floor
    PASS  no tenant can take the whole gateway            3 of 3 have a ceiling
    PASS  operational endpoints are protected             admin key configured
    PASS  a reserved floor protects its tenant            'acme' held 2.38 of 2.36 req/s, queue wait p95 322ms
    PASS  traffic survives a provider failure             100% served while 'fast' was down, 25 failovers
    PASS  a failing provider is taken out of rotation     3 requests skipped 'fast' after 25 failures
    PASS  shutdown finishes running work                  2 in flight finished in 0.8s, 0 queued refused
    PASS  load balancers are told to stop sending         /health returned 503 while draining
    PASS  no capacity is left held                        0 in flight, 0 queued at rest
    PASS  scheduler invariants held                       capacity, queue and budget accounting stayed within limits

  All guarantees held.
```
