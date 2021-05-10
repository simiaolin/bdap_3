import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;


public class Exercise1_xg {
    public static void main(String[] args) {
        SparkConf spark_conf = new SparkConf().setAppName("SparkTripDistribution").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(spark_conf);
        jsc.setLogLevel("WARN");

        // load data
        long start_time = System.currentTimeMillis();
        JavaRDD<String> file_RDD = jsc.textFile(args[0], 1);

        // handle data
        JavaRDD<Integer> distances_RDD = file_RDD.map((line)->{
            String[] trip_info = line.split(" ");
            double trip_distance = Calculations.sphericalEarthFlatDistance(trip_info[2], trip_info[3], trip_info[5], trip_info[6]);  // km
            double duration = Double.parseDouble(trip_info[4]) - Double.parseDouble(trip_info[1]);  // seconds
            double speed = Calculations.getSpeed(trip_distance, duration);  // km/h
            if (speed <= 200 || duration == 0) {
                return (int) Math.round(trip_distance);
            }
            return null;
        });

        // every distance counts 1
        JavaPairRDD<Integer, Integer> distance_RDD = distances_RDD.mapToPair(distance -> new Tuple2<>(distance, 1));
        // counting
        JavaPairRDD<Integer, Integer> distanceCount_RDD = distance_RDD.reduceByKey(Integer::sum);

        long end_time = System.currentTimeMillis();

        // print the result
        List<Tuple2<Integer, Integer>> result = distanceCount_RDD.collect();
        result.forEach(System.out::println);

        // close jsc
        jsc.stop();
        System.out.println("Total time is: " + (end_time-start_time) + "ms.");
    }

    public static class Calculations {
        private static final double pi = Math.PI;
        private static final double rad = pi / 180;
        private static final double r = 6371.009;  // km

        public static double sphericalEarthFlatDistance(String splat, String splong, String eplat, String eplong) {
            double start_pos_lat = Double.parseDouble(splat) * rad;
            double start_pos_long = Double.parseDouble(splong) * rad;
            double end_pos_lat = Double.parseDouble(eplat) * rad;
            double end_pos_long = Double.parseDouble(eplong) * rad;
            double delta_fai = end_pos_lat - start_pos_lat;
            double fai_m = (end_pos_lat + start_pos_lat) / 2;
            double delta_lambda = end_pos_long - start_pos_long;
            return r * Math.sqrt(Math.pow(delta_fai, 2) + Math.pow(Math.cos(fai_m)*delta_lambda, 2));
        }

        public static double getSpeed(Double dist, Double duration) {
            return (3600*dist) / duration;
        }
    }
}
