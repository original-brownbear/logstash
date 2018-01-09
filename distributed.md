From yesterdays presentation:


# Introduction - Execution Flow Overview

See below Diagram for a design that involves P2P orchestration, using ES as the persistence layer.

![diagram-3499148624186078969](https://user-images.githubusercontent.com/6490959/33259037-cfbfe5f4-d35b-11e7-9826-a2addd03ff98.png)

* Minority cannot update ES shared state
* Minority can use ES to persist their individual append-only logs
* No transactionality in ES required, synchronization happens P2P
* Leader loss triggers repartitioning of tasks (guarantees at least once)
* ES does:
  * Bootstrapping/discovery, configuration and persisting of distributed state snapshot
* P2P Communication does:
  * Orchestration of execution
  * Synchronization of ES writes (except for failover append-only log persistence on leader loss)
* Not (fully) illustrated for simplicity:
  * Loss of follower -> just repartition in version 1
  * Joining follower -> Either repartition (when there's no windowing, this is what's illustrated) or just assign next task (windowing)

# 3 Things Need Implementation

## Task Partitioning

This is heavily dependant on plugin implementation wise. Logically we could identify three logical
categories of plugins:

1. Can’t be partitioned (TCP Input, UDP input, HTTP input I would exclude file input from this 
category since we could still document and implement this based on NFS trivially
2. Doesn’t require partitioning (just supervision) in some configurations
   * Examples:
      * Kafka
      * RabbitMQ
      * HTTP Poller
   * Leader assigns partitions to followers after partitioning once (repartition only on node topology changes)
   * Followers report state back to leader (implicit heartbeat)
   * Leader stops execution, repartitions and restarts execution on node join/leave
3. Requires partitioning and windowing (at least in some configurations)
   * Examples:
      * Kafka (manual partition assignment), HTTP Poller (set specific metadata)
      * S3
      * (NFS)
   * Leader continuously assigns tasks to followers
   * Followers leaving trigger reassignment of their task after a timeout
   * Followers joining are assigned new tasks as they (new tasks) become available
  
## Task Serialization

* Serializing simple config like data structures only won’t work when we need windowing and will
require implementing APIs for reporting state back to the master.
* Serialize Lambdas for the inputs instead and have plugins deal with coordination
Provide serializable distributed Semaphore/Lock and StatusReporter
* Allow running lambdas on Slaves via slave.execute(inputLambda) => ExecutionFuture<Input>
(with handlers for failures, done and the ability to cancel) with master node maintaining a list of
slaves (very ergonomic, because code can essentially be written like multi-threaded code, you only
need to work with serializable variables only in the lambdas which as proven by Spark isn’t a
practical issue if you provide serializable concurrency primitives)
   * Needs a static way to get a hold of the Queue inside the Lambdas (trivial)
* Serialized tasks/lambdas flow over P2P network layer

### Example Code Executed on Leader

#### S3
```java
while(true) {
    slaves  = getIdleSlaves(); // Blocks until slave becomes available 
    slaveCount = slaves.length;
    files = getOutstandingS3Files(slaveCount) // block & ret. max. #slaveCount files
    i = 0;
    context = getContext(); // holds short-lived state and is serializable
    for (file : files) {
        slaves[i++].submit(
                ()  -> {downloadFileToLsQueue(context, file)); commitToEs(file);} // Runs on Slave
            ).onFailureOrTimeout(
                () ->{
                    slaves[i % slavecount].markDead(); // Runs on Master and persists information in ES
                    reEnqueue(file); // same here, master persists the state to ES
           }
     );
    }
}
```


#### Kafka

```java
while(true) {
	context = getContext(); // holds short-lived state and is serializable
	for (slave : getIdleSlaves()) { // Runs on Master
	    slaves[i++].submit(
	    () -> runKafkaInput(context).onFailureOrTimeout() // Runs on Slave
	    () -> slaves[i % slavecount].markDead()
     );
  }
}

```

## Distributed State

* Advertise peers in ES to allow for bootstrapping if all LS nodes go down
* Store configuration in ES to bootstrap cluster
* Store “offsets” (e.g. files already process) to ES for persistence when all LS nodes go down
* Elect leader from peers registered in ES via p2p leader election
    * All in all you would only need to configure the correct ES coordinates/credentials on each LS node
    * Leader election and p2p consensus can be implemented using [RAFT](https://raft.github.io)
* Make leader run actual plugin code illustrated above according to a configuration stored in ES

### Example Distributed Set for storing already processed files for S3

* All writes and reads to ES are executed the (RAFT-)leader if a healthy leader is available
    * All LS nodes communicate their writes (fully processed file names) to the leader
    * If leader goes away nodes can persist their updates for in-progress tasks to the set in form of a write-ahead-log as single ES documents per LS node
    instead of writing to the leader, so that a newly elected leader can regenerate the cluster state once its elected 
* Writes to ES can be made consistent by using ES versions with the RAFT term as external version-mechanism
* Leader stores and persists serialized set to ES
   * Large sets can be partitioned and serialized into multiple ES docs, smaller sets can be
   serialized into a single blob (since only the leader ever writes to ES we don't need any tricks
   for non-atomic operations on the set)
       * Note: Partitioning a set is just a theoretical problem anyways, in reality any practically viable set of already read paths will fit into a radix/PATRICIA-trie that can be serialized as a single ES doc in any case
   * Sets too large for storage on heap can use a bloom-filter for caching on the leader
* This can easily be implemented as a `Serializable` to transparently work over the wire when used
in code like the above by having the ES coordinates and leader coordinates for the set as non-transient fields in the implementation 


### Extremal Single Node Case

* Either for non-parallelizable inputs or if no clustering is configured
* Use on-disk flat-file as state store instead of ES
   * Can use FS locking to ensure exclusivity here and put the same API in front of ES version handling
   * We require a clean flat-file persistence layer for the ES-backed distributed stated as well.
   Without it, the behaviour of exceptions in the Leader-ES communication becomes very complex. 
* Run leader and slave inside the same JVM

## Q & A

### Why RAFT?

* Simple and fast
* We need heart-beating the `master`/`leader` for our business logic -> using a RAFT approach gives us heartbeats exactly the way we need them to monitor worker nodes.
* Rafts term concept neatly allows us to persist the state to ES via ES's versioning
* Persistent (ES) state updates only flow form the leader to the workers 
   * Easy to implement if we want to store part of the state in ES 
   * Workers can read if their term agrees with the data version in ES
   * Only leader at the correct term can write to ES
 
### Why not Handle Orchestration via ES

* Would require polling ES for updates
* Things like leader election would involve at least twice as many network trips
* We don't have a number of LS nodes involved that prohibits each Node from connecting
to all other known Nodes
* Detecting a Node going down would require constantly polling ES for heartbeats from that node
* Atomic operations are very hard across multiple ES documents
