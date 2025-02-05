# boro-db
simple database written in golang

# Plan
- [x] heap file management system
- [ ] write ahead log system on top of heap
- [ ] file managements system using WAL for crash recovery for directory records
- [ ] page buffer manager
- [ ] crash recovery system (ARES) ?
- [ ] Records management system on top of page system
- [ ] Skip List index for page system
- [ ] multi thread / shard per core architecture implementation
- [ ] transaction support (decide isolation system)
- [ ] index + page garbage collection
- [ ] I/O multiplex and io-uring usage
- [ ] data compression and layout
- [ ] Data structure support for keys - List, Set, HashMap , Sorted Set
- [ ] Structured record manager
- [ ] Query engine
