# PSMR
boost smr with parallel execution:
- early model: decision to execute the command in parallel is made at the leader
- late model: decision to execute the command in parallel is made at the each follower
- Parallel execution strategy: break the limitation of command sequence execution based on paxos/raft, used hash+queue to construct DAG logically
## dependency
- raft package used in etcd
- redis
- goredis
## todo
- [ ] performance tests
- [ ] code review
- [ ] integrate into Docker/k8s

## environment
- 4 HP nodes with four-way E7-4820 CPU, 2.0GHz, 8 cores per channel, a total of 64 threads

## else
<font color="red">only used for validation of algorithm correctness and basic performance testing, not for production environment</font>
