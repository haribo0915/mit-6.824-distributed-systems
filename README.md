## MIT 6.824: Distributed Systems

> Labs of [MIT 6.824 Spring 2020](http://nil.csail.mit.edu/6.824/2020/)

### Course Description

MIT 6.824 is a core 12-unit graduate subject with lectures, readings, programming labs, an optional project, a mid-term exam, and a final exam. It will present abstractions and implementation techniques for engineering distributed systems. Major topics include fault tolerance, replication, and consistency. Much of the class consists of studying and discussing case studies of distributed systems.

[![GitHub go.mod Go version of a Go module](https://img.shields.io/github/go-mod/go-version/gomods/athens.svg)](https://github.com/gomods/athens)

#### Lab1 MapReduce
* [MapReduce framework](https://static.googleusercontent.com/media/research.google.com/zh-TW//archive/mapreduce-osdi04.pdf) consists of a coordinator process that hands out tasks to workers and copes with failed workers for big data processing on large clusters.
- [X] Coordinator process
- [X] Worker process

#### Lab2 Raft
* [Raft](https://raft.github.io/raft.pdf) is a consensus algorithm for managing a replicated
  log. It ensures that all the replica servers see the same log. Each replica executes client requests in log order, applying them to its local copy of the service's state.  If a server fails but later recovers, Raft takes care of bringing its log up to date. Raft will continue to operate as long as at least a majority of the servers are alive and can talk to each other. If there is no such majority, Raft will make no progress, but will pick up where it left off as soon as a majority can communicate again.
- [X] Part A - Raft leader election and heartbeats
- [X] Part B - Append new log entries
- [X] Part C - Write Raft's persistent state each time it changed, and read the state back when restarting after a reboot

#### Lab3 Fault-tolerant Key/Value Service
*  A strongly consistent replication database based on Raft consensus algorithm, which supports Get/Put/Append operations and is available under all non-Byzantine conditions as long as a majority of the servers are operational and can communicate with each others.
- [X] Part A - Key/Value service without log compaction
- [X] Part B - Key/Value service with log compaction

#### Lab4 Sharded Key/Value Service
* A database with architecture resembling Google BigTable for processing client operations in parallel on a set of replica groups to enhance performance; the system is able to shift the assignment of shards among replica groups automatically for load balancing.
- [X] Part A - Shard Master
- [X] Part B - Sharded Key/Value Server
- [ ] Challenge1 - Garbage collection of state
- [ ] Challenge2 - Client requests during configuration changes