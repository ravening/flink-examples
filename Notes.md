# Concepts

Flink means fast in German

storage and cluster agnostic. machine learning, graph processing support

4 v's of big data

volume, variety, velocity, veracity

Mapreduce

data stored in distributed filesystem

map - convert input 
reduce - reduce the data to common format


Flink contains simpler directed acyclic graph model

Unique flink features

1. Batch is special case 
2. Flexible windowing 
3. Iterations support 
4. Memory management (uses java byte array)  
5. Supports exactly once processing 
6. Query optimization 

## Architecture

Low level is the datasources like files, databases and streams 
next level ins the execution environment like local, cluster(yarn, mesos) or in cloud 
next level is core layer for distributed streaming dataflow runtime 
on top of it is batch and stream api's 
top level is the libraries 


Types of windows

Tumbling, Sliding, count, Session and global


Tumbling - Time is fixed and the window doesnt overlap 
window size is measured in time. like 20 seconds 
entity which belongs to 1 window doesnt belong to another 


Sliding - window size is fixed with a overlapping time 


Count - window size is based on the entities count in the window. 
can have overlapping or non overlapping entities 
Number of entities always remain same 

They are applied on KeyedStreams. applies on per key basis 


Session - window size is based on session data. 
no concept of overlapping time. 
number of entities differ in a window. 
session gap determines the window size. 
Like if you are idle for 2 minutes then a new session is started.



Global - all entities belong to some window. not much interesting



States in flink is maintaines using operators and keyed state. 

Raw state - uses general data structures which flink is unaware of 
Checkpoints are written as bytes. 


Managed state - specific data structures which flink knows about and know how to serialize/deserialize 
- ValueState, ListState and ReduceState. 

flink encodes these when checkpoints are written.

actual state can be in memory or in disk or in db. 


Rich functions are used to maintain the state. any transformation which extends the rich function \
is called as rich transformation. It gets its value using the RuntimeContext.



When checkpointing is not enabled then app wont be restarted on failured. 
Delay restart will restart app after failure when checkpoint is enabled. Default is int max value. 
failure rate - tracks how many times app failed to restart. if it crosses threshold then restart is not attempted. 




