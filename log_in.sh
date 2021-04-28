#!/bin/sh -v
eval `ssh-agent -s` && ssh-add ~/.ssh/id_rsa
scp /Users/ary/dev/code/trait/bdap_3/src/main/java/ConstructionTrip.java r0829520@ham.cs.kotnet.kuleuven.be:/home/r0829520/assign3/revenueCalculation/
ssh r0829520@bilzen.cs.kotnet.kuleuven.be

#cd /cw/bdap/assignment3/
#ssh r0829520@orval.cs.kotnet.kuleuven.be

cd /home/r0829520/assign3/revenueCalculation
rm *.class *.jar
rm -rf tmp final tmp_2010 final_2010

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export SPARK_INSTALL=/cw/bdap/software/spark-2.4.0-bin-hadoop2.7
export HADOOP_INSTALL=/cw/bdap/software/hadoop-3.1.2
export PATH=$PATH:$HADOOP_INSTALL/bin:$HADOOP_INSTALL/sbin
export HADOOP_CONF_DIR=/localhost/NoCsBack/bdap/clustera
#export HADOOP_CONF_DIR=/localhost/NoCsBack/./bdap/clusterb

hadoop fs -ls /user/r0829520
#hadoop fs  -ls /tmp/logs/r0829520/logs/application_1619513266629_0001
#hadoop fs  -cat /tmp/logs/r0829520/logs/application_1619513266629_0001

#hadoop fs -copyFromLocal /home/r0829520/assign3/data/2010_03.segments  /user/r0829520

javac -cp $(yarn classpath) ConstructionTrip.java
jar cf ConstructionTrip.jar *.class
hadoop jar ConstructionTrip.jar ConstructionTrip /user/r0829520/2010_03.segments  /user/r0829520/tmp_2010_3 /user/r0829520/final_2010_3 1000000 15
hadoop jar WordCount.jar WordCount file:///home/r0829520/assign3/data/hello /user/r0829520/hello

#$SPARK_INSTALL/bin/spark-submit --class "WordCount" --master local[1] WordCount.jar  data/hello file:///home/r0829520/assign3/wordcount/out


javac -cp $(yarn classpath) ConstructionTrip.java
jar cf ConstructionTrip.jar *.class
$SPARK_INSTALL/bin/spark-submit --class "ConstructionTrip" --master local[1] ConstructionTrip.jar  ../data/taxi_706.segments file:///home/r0829520/assign3/revenueCalculation/tmp file:///home/r0829520/assign3/revenueCalculation/final
$SPARK_INSTALL/bin/spark-submit --class "ConstructionTrip" --master local[1] ConstructionTrip.jar  ../data/2010_03.segments file:///home/r0829520/assign3/revenueCalculation/tmp_2010 file:///home/r0829520/assign3/revenueCalculation/final_2010

vim /tmp/hadoop-r0829520/mapred/local/localRunner/r0829520/job_local2077911187_0001


vim  /tmp/logs/r0829520/logs/application_local2077911187_0001

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