# k-means-parallel
The project was developed for university course _Data mining_. The purpose of this project was to implement parallel 
version of k-means clustering algorithm. 

## Parallelism 
Parallelism was achieved by parallel computation of Euclidean distances between points in data set. Distances may be 
computed independently of each other. In the end of each iteration, computed values are saved to dedicated structure.

For parallelization, Spark engine was used. Conducted experiments showed, that Spark isn't the best option for 
parallelization of k-means algorithm.

## Building & Running
User may adjust program settings by modifying `application.conf` file.

## Used technologies
* Scala 2.12
* Spark 2.3

## Example result
[Result](resources/result.PNG)

