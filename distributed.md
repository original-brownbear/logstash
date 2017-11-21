My plan for implementing this would be to use distributed data structures and serializable closures
in a way similar to how Apache Spark works. 

# 3 Things Need Implementation

## Task Partitioning

This is heavily dependant on plugin implementation wise. Logically we could identify three logical
categories of plugins:

1. Can’t be partitioned (TCP Input, UDP input, HTTP input I would exclude file input from this 
category since we could still document and implement this based on NFS trivially
2. Doesn’t require partitioning (just supervision) in some configurations
   * Kafka
   * RabbitMQ 
   * HTTP Poller
3. Requires partitioning and Windowing (at least in some configurations)
   * Kafka (manual partition assignment), HTTP Poller (set specific metadata)
   * S3
   * (NFS)
  
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


### Example Code

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

* Advertise peers in ES
* Store configuration in ES
* Store “offsets” (e.g. files already process) to ES
* Elect leader from peers registered in ES via p2p leader election
    * All in all you would only need to configure the correct ES coordinates/credentials on each LS node
* Make leader run actual plugin code illustrated above according to a configuration stored in ES

### Example Distributed Set for storing already processed files for S3

* All writes and reads to ES are executed the leader if a healthy leader is available
    * All LS nodes communicate their writes (fully processed file names) to the leader
    * If leader goes away nodes can persists their updates to the set in form of a write-ahead-log as single ES documents per LS node
    instead of writing to the leader, so that a newly elected leader can regenerate the cluster state once its elected 
* Leader stores persists serialized set to ES
   * Large sets can be partitioned and serialized into multiple ES docs, smaller sets can be
   serialized into a single blob (since only the leader ever writes to ES we don't need any tricks
   for non-atomic operations on the set)
   * Sets too large for storage on heap can use a bloom-filter for caching on the leader
* This can easily be implemented as a `Serializable` to transparently work over the wire when used
in code like the above by having the ES coordinates and leader coordinates for the set as non-transient fields in
the implementation 

## Extremal Single Node Case

* Either for non-parallelizable inputs or if no clustering is configured
* Use on-disk flat-file as state store instead of ES
* Run leader and slave inside the same JVM
