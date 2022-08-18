# VERY MUCH Proof-of-concept / WIP
Rate Limiting backend using Redis


## TODO:

* [ ] Implement Partitioned TokenBucket RL which performs batching
* [ ] Implement Partitioned "Approximate" TokenBucket RL which performs batching and caches decisions until next refresh
  * In practice, we imagine two-level RL: a local one with a lower limit configured by the user (to account for multiple clients), and a global one which sends batches async to Redis
