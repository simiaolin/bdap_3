### Preparation commands
```shell
export SPARK_INSTALL=/cw/bdap/software/spark-2.4.0-bin-hadoop2.7
export HADOOP_INSTALL=/cw/bdap/software/hadoop-3.1.2
export PATH=$PATH:$HADOOP_INSTALL/bin:$HADOOP_INSTALL/sbin
export HADOOP_CONF_DIR=/localhost/NoCsBack/bdap/clustera
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
```



### Exercise1

- Compile code
  ```shell
  javac -cp /cw/bdap/software/spark-2.4.0-bin-hadoop2.7/jars/spark-core_2.11-2.4.0.jar:/cw/bdap/software/spark-2.4.0-bin-hadoop2.7/jars/scala-library-2.11.12.jar Exercise1.java
  jar cf Exercise1.jar *.class
  ```

- Execute code

  ```shell
  $SPARK_INSTALL/bin/spark-submit --class "Exercise1" --master local[1] Exercise1.jar spark /home/r0829520/assign3/data/2010_03.trips  /home/r0829520/assign3/out/spark_3
  ```

- Meaning of params for Exercise1.jar
  1. whether the job is run on spark or not. 
     - spark -> run on spark
     - simple -> run on normal java
  2. input file path
  3. output file path

### Exercise2

- Compile code
  ```shell
  jar cf Exercise2.jar *.class
  javac -cp $(yarn classpath) Exercise2.java
  ```

- Execute code

  ```shell
  hadoop jar Exercise2.jar Exercise2 /data/all.segments /user/r0829520/all_tmp /user/r0829520/all_final 256000000 20 32000000 1  trip_reconstruction airport_revenue_distribution
  ```

- Meaning of params for Exercise2.jar
  1. input file path
  2. output file path for first hadoop job
  3. output file path for second(final) hadoop job
  4. split size of mapper of first hadoop job (to control the number of mappers of first hadoop job)
  5. number of reducers of first hadoop job
  6. split size of mapper of second hadoop job (to control the number of mappers of second hadoop job)
  7. number of reducers of second hadoop job
  8. name of first hadoop job
  9. name of second hadoop job



### Contact 

If there is anything unclear, please feel free to contact simiao.lin@student.kuleuven.be