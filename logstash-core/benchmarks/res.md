ACK = 1000

# Run complete. Total time: 00:31:42

Benchmark                                     Mode  Cnt     Score     Error   Units
QueueRWBenchmark.readFromArrayBlockingQueue  thrpt   10  3311.727 ± 914.724  ops/ms
QueueRWBenchmark.readFromMemoryQueue         thrpt   10   143.646 ±  11.274  ops/ms
QueueRWBenchmark.readFromPersistedQueue      thrpt   10    72.388 ±   2.286  ops/ms
QueueWriteBenchmark.pushToPersistedQueue     thrpt   10   114.926 ±  13.952  ops/ms
fs.FSyncGCPressure.GROUP                     thrpt   30     0.850 ±   0.024   ops/s
fs.FSyncGCPressure.GROUP:actionBenchmark     thrpt   30     0.422 ±   0.003   ops/s
fs.FSyncGCPressure.GROUP:fsync               thrpt   30     0.428 ±   0.024   ops/s
fs.MSyncGCPressure.GROUP                     thrpt   30     1.032 ±   0.176   ops/s
fs.MSyncGCPressure.GROUP:actionBenchmark     thrpt   30     0.437 ±   0.002   ops/s
fs.MSyncGCPressure.GROUP:msync               thrpt   30     0.595 ±   0.176   ops/s

