import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Exercise1 {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            throw new Exception("please specify 3 arguments \n" +
                    "[1]: type of calculating trip distribution [simple] or [spark] \n" +
                    "[2]: input file path \n" +
                    "[3]: output file path \n");
        }

        long startTime = System.currentTimeMillis();

        if (args[0].toLowerCase().equals("simple")) {
            SimpleTripDistribution.execTripDistribution(args[1], args[2]);
        } else if (args[0].toLowerCase().equals("spark")) {
            SparkTripDistribution.execTripDistribution(args[1], args[2]);
        } else {
            throw new Exception("calculating type should be [simple] or [spark]");
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Use time: " + (endTime - startTime) + "ms.");
    }

}

class SimpleTripDistribution{
    public static void execTripDistribution(String input, String output) throws IOException {
        List<String> lines = readTripFile(input);
        HashMap<Integer, Integer> distribution = getDistanceDistribution(lines);
        writeTripLength(output, distribution);
    }

    public static List<String> readTripFile(String input) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(input));
        List<String> lines = new ArrayList<>();
        String line = br.readLine();
        while (line != null) {
            lines.add(line);
            line = br.readLine();
        }
        br.close();
        return lines;
    }

    public static void writeTripLength(String output, Map<Integer, Integer> distribution) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(output));
        for (Map.Entry<Integer, Integer> dtb : distribution.entrySet()) {
            bw.write(dtb.getKey() + "," + dtb.getValue());
            bw.newLine();
        }
        bw.close();
    }


    public static HashMap<Integer, Integer> getDistanceDistribution(List<String> lines) {
        HashMap<Integer, Integer> distribution = new HashMap<>();
        for (String line : lines) {
            String[] columns = line.split(" ");
            Double startLat = Double.valueOf(columns[2]);
            Double startLong = Double.valueOf(columns[3]);
            Double endLat = Double.valueOf(columns[5]);
            Double endLong = Double.valueOf(columns[6]);
            Integer distance = (int) Math.round(DistanceUtilOne.getSphericalProjectionDistance(startLat, startLong, endLat, endLong));
            Double beginTime = Double.valueOf(columns[1]);
            Double endTime = Double.valueOf(columns[4]);
            Double tripTime = endTime - beginTime;
            Double speed = 3.6 * distance / tripTime;
            if (speed < 300.0) {     //filter on speed
                if (distribution.containsKey(distance)) {
                    distribution.put(distance, distribution.get(distance) + 1);
                } else {
                    distribution.put(distance, 1);
                }
            }
        }
        return distribution;
    }
}

class SparkTripDistribution {
    public static void execTripDistribution(String input, String output) throws IOException {
        SparkConf conf = new SparkConf().setAppName("TripDistribution").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);
        context.setLogLevel("WARN");

        //map each trip to tuple <distance, 1>
        JavaRDD<String> tripRDD = context.textFile(input);
        JavaPairRDD<Integer, Integer> distancesRDD = tripRDD
                .mapToPair(line -> DistanceUtilOne.getDistanceAndSpeedTuple(line))     // line -> <distance, speed>
                .filter(tripAndSpeedTuple -> tripAndSpeedTuple._2 < 300.0)          // filter on speed < 300.0
                .mapToPair(tripAndSpeedTuple -> new Tuple2<>(tripAndSpeedTuple._1, 1)); // <distance, speed> -> <distance,1>


        //count on every distance
        JavaPairRDD<Integer, Integer> distanceDistributionRDD = distancesRDD.reduceByKey(Integer::sum);

        //write the result to file
        writeTripLength(output, distanceDistributionRDD.collect());
        context.stop();
    }

    public static void writeTripLength(String output,   List<Tuple2<Integer, Integer>> distribution) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(output));
        distribution.forEach(e -> {
                    try {
                        bw.write(e._1 + "," + e._2);
                        bw.newLine();
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                }
        );

        bw.close();
    }

}


class DistanceUtilOne {
    private static final double R = 6371.009;

    public static Tuple2<Integer, Double> getDistanceAndSpeedTuple(String line) {
        String[] trip = line.split(" ");
        Double distance = DistanceUtilOne.getSphericalProjectionDistance(
                Double.valueOf(trip[2]), Double.valueOf(trip[3]), Double.valueOf(trip[5]), Double.valueOf(trip[6]));
        Double interval = Double.valueOf(trip[4]) - Double.valueOf(trip[1]);
        Double speed = DistanceUtilOne.getSpeed(distance, interval);
        return new Tuple2<>((int) Math.round(distance), speed);
    }

    public static Double getSphericalProjectionDistance(double startLat, double startLong, double endLat, double endLong) {
        Double deltaLat = (endLat - startLat);
        Double deltaLong = (endLong - startLong);
        Double midLat = (startLat + endLat) / 2 * Math.PI / 180;
        return R * Math.PI * Math.sqrt(Math.pow(deltaLat, 2) + Math.pow(Math.cos(midLat) * deltaLong, 2)) * 1000 / 180;
    }

    public static Double getSpeed(double distance, double tripTime) {
        double speed = 3.6 * distance / tripTime;
        return speed;
    }
}

