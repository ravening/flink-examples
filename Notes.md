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

