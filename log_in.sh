#!/bin/sh -v
eval `ssh-agent -s` && ssh-add ~/.ssh/id_rsa
ssh r0829520@ham.cs.kotnet.kuleuven.be
cd /cw/bdap/assignment3/
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export SPARK_INSTALL=/cw/bdap/software/spark-2.4.0-bin-hadoop2.7
export HADOOP_INSTALL=/cw/bdap/software/hadoop-3.1.2
export PATH=$PATH:$HADOOP_INSTALL/bin:$HADOOP_INSTALL/sbin
javac -cp $(yarn classpath) WordCountNew.java
jar cf WordCountNew.jar *.class
$SPARK_INSTALL/bin/spark-submit --class "WordCountNew" --master local[1] WordCountNew.jar  data/hello file:///home/r0829520/assign3/out
$SPARK_INSTALL/bin/spark-submit --class "WordCount" --master local[1] WordCount.jar  data/hello file:///home/r0829520/assign3/out

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
