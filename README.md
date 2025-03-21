# boro-db
simple database written in golang

# Plan
- [x] heap file management system
    - [ ] add benchmark test suite
    - [ ] add I/O uring for file reads 
- [x] page buffer manager
    - [ ] change interface for batched writes + reads
    - [ ] vectorized reads + writes with IO Uring
    - [ ] page pool creation with page buffer
- [ ] write ahead log system on top of heap for Physical logging
- [x] File system interface
    - [ ] investigate bottlenecks of poor locks usage, lockless maps ? lock less lists ? improve LRU please !
    - [ ] check if heap modifications can be lock free and atleast mallocs / free / checks can be lock free
- [ ] KV using fs interface
    - [ ] lsm using pager + heap
    - [ ] b+ tree for indexes alone
