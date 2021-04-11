package example;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;


import scala.Tuple2;

public final class WordCountRemote {


    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Spark Count");
        sparkConf.set("spark.master", "local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);


        JavaRDD<String> textFile = sc.textFile(args[0]);

        JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });
        counts.saveAsTextFile(args[1]);
        System.exit(0);
    }
}
