#!/bin/sh -v
eval `ssh-agent -s` && ssh-add ~/.ssh/id_rsa
scp /Users/ary/dev/code/trait/bdap_3/src/main/java/ConstructionTrip.java r0829520@ham.cs.kotnet.kuleuven.be:/home/r0829520/assign3/revenueCalculation/
scp /Users/ary/dev/code/trait/bdap_3/src/main/java/SparkTripLengthDistribution.java r0829520@ham.cs.kotnet.kuleuven.be:/home/r0829520/assign3/sparkTripLength/
scp /Users/ary/dev/code/trait/bdap_3/src/main/java/example/WordCount.java r0829520@ham.cs.kotnet.kuleuven.be:/home/r0829520/assign3/wordcount/
ssh r0829520@bilzen.cs.kotnet.kuleuven.be
#ssh r0829520@beringen.cs.kotnet.kuleuven.be

#cd /cw/bdap/assignment3/
#ssh r0829520@orval.cs.kotnet.kuleuven.be

cd /home/r0829520/assign3/revenueCalculation
rm *.class *.jar

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export SPARK_INSTALL=/cw/bdap/software/spark-2.4.0-bin-hadoop2.7
export HADOOP_INSTALL=/cw/bdap/software/hadoop-3.1.2
export PATH=$PATH:$HADOOP_INSTALL/bin:$HADOOP_INSTALL/sbin
export HADOOP_CONF_DIR=/localhost/NoCsBack/bdap/clustera
javac -cp $(yarn classpath) ConstructionTrip.java
javac -cp $(yarn classpath) SparkTripLengthDistribution.java
jar cf ConstructionTrip.jar *.class
jar cf SparkTripLengthDistribution.jar *.class
#export HADOOP_CONF_DIR=/localhost/NoCsBack/./bdap/clusterb

hadoop fs -ls /user/r0829520
hadoop fs -ls /data

#hadoop fs  -ls /tmp/logs/r0829520/logs/application_1619513266629_0001
#hadoop fs  -cat /tmp/logs/r0829520/logs/application_1619513266629_0001


hadoop jar ConstructionTrip.jar ConstructionTrip /data/taxi_706.segments  /user/r0829520/706_tmp_5 /user/r0829520/706_final_5 100000 3
hadoop jar ConstructionTrip.jar ConstructionTrip /data/2010_03.segments  /user/r0829520/tmp_2010_5 /user/r0829520/final_2010_5 1000000 10
hadoop jar ConstructionTrip.jar ConstructionTrip /data/all.segments  /user/r0829520/all_tmp_4 /user/r0829520/all_final_4 128000000 10 42000000 1 trip_128_42 revenue_128_42

$SPARK_INSTALL/bin/spark-submit --class "SparkTripLengthDistribution" --master local[1] SparkTripLengthDistribution.jar /data/2010_03.trips   /user/r0829520/spark_1

hadoop job -list
hadoop job -kill app_

/data/2010_03.segments   1.2gb
/data/2010_03.trips
/data/all.segments       29gb 128000000 10 42000000 1  1: 13.5m  2: 1recudes 1m
/data/all.segments       29gb 128000000 1: 230maps+10reduces 12m  2: 10maps+10recudes 1m
/data/taxi_706.segments  1.8m 100000    19jobs 18s

hadoop fs -copyFromLocal /home/r0829520/assign3/data/hello  /user/r0829520/hello
hadoop jar WordCount.jar WordCount /user/r0829520/hello /user/r0829520/hello_out 3

#$SPARK_INSTALL/bin/spark-submit --class "WordCount" --master local[1] WordCount.jar  data/hello file:///home/r0829520/assign3/wordcount/out


javac -cp $(yarn classpath) ConstructionTrip.java
jar cf ConstructionTrip.jar *.class
$SPARK_INSTALL/bin/spark-submit --class "ConstructionTrip" --master local[1] ConstructionTrip.jar  ../data/taxi_706.segments file:///home/r0829520/assign3/revenueCalculation/tmp file:///home/r0829520/assign3/revenueCalculation/final
$SPARK_INSTALL/bin/spark-submit --class "ConstructionTrip" --master local[1] ConstructionTrip.jar  ../data/2010_03.segments file:///home/r0829520/assign3/revenueCalculation/tmp_2010 file:///home/r0829520/assign3/revenueCalculation/final_2010




scp /Users/ary/dev/code/trait/bdap_3/src/resources/input/2010_03.segments r0829520@ham.cs.kotnet.kuleuven.be:/home/r0829520/assign3/data
scp /Users/ary/dev/code/trait/bdap_3/src/resources/input/taxi_706.segments r0829520@ham.cs.kotnet.kuleuven.be:/home/r0829520/assign3/data

#scp -r /Users/ary/CLionProjects/bdap_2/ r0829520@balen.cs.kotnet.kuleuven.be:/home/r0829520/assign2/

ssh r0829520@ham.cs.kotnet.kuleuven.be

ssh -L 8080:mysql.cs.kotnet.kuleuven.be:80 r0829520@st.cs.kuleuven.be
ssh r0829520@ans.cs.kotnet.kuleuven.be
ssh r0829520@heers.cs.kotnet.kuleuven.be
ssh r0829520@hasselt.cs.kotnet.kuleuven.be
ssh r0829520@ohey.cs.kotnet.kuleuven.be
ssh r0829520@knokke.cs.kotnet.kuleuven.be
ssh r0829520@waterloo.cs.kotnet.kuleuven.be
ssh r0829520@yvoir.cs.kotnet.kuleuven.be
ssh r0829520@ham.cs.kotnet.kuleuven.be
ssh r0829520@gent.cs.kotnet.kuleuven.be
ssh r0829520@brugge.cs.kotnet.kuleuven.be

dmesg | grep "killed"

ssh r0829520@lommel.cs.kotnet.kuleuven.be
ssh r0829520@fleurus.cs.kotnet.kuleuven.be
ssh r0829520@komen.cs.kotnet.kuleuven.be
ssh r0829520@asse.cs.kotnet.kuleuven.be
ssh r0829520@musson.cs.kotnet.kuleuven.be
ssh r0829520@alken.cs.kotnet.kuleuven.be

#kill heers balen (1) gent waterloo（0）knokke (1) yvoir(1) ans(1)

#ohey hasselt ham brugge

cd /usr/local/Cellar/apache-spark/3.1.1




https://10.33.14.40:8188/gateway/yarnui/yarn/apps/RUNNING
https://10.33.14.40:8443/gateway/yarnui/yarn/apps/RUNNING

ssh -L 8081:10.33.14.40:8188 bilzen.cs.kotnet.kuleuven.be