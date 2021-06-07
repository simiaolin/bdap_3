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

    /** To compute the trip distance distribution, two ways are provided -- the simple algorithm, and the spark solution.
     * @param args     Three params are needed. The meaning of the params are shown below.
     *                 1. whether the job is run on spark or not.
     *                      - spark -> run on spark
     *                      - simple -> run on normal java
     *                 2. input file path
     *                 3. output file path
     * @throws Exception
     */
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

/**
 * SimpleTripDistribution is a simple program to compute the trip distance distribution.
 */
class SimpleTripDistribution{

    /**
     * Given a file recording trip information, write out the trip distance distribution.
     * @param input         path of file containing trip records
     * @param output        path of file to write out the trip distance distribution
     * @throws IOException
     */
    public static void execTripDistribution(String input, String output) throws IOException {
        List<String> lines = readTripFile(input);
        HashMap<Integer, Integer> distribution = getDistanceDistribution(lines);
        writeTripLength(output, distribution);
    }

    /**
     * Read lines from a file.
     * @param input   path of input file
     * @return        a list of lines in the input file
     * @throws IOException
     */
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

    /**
     * Write the trip distance distribution to the output file
     * @param output         path of output file
     * @param distribution   a map of trip distance distribution, where the KEY is the trip distance, and the VALUE is the count of the distance.
     * @throws IOException
     */
    public static void writeTripLength(String output, Map<Integer, Integer> distribution) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(output));
        for (Map.Entry<Integer, Integer> dtb : distribution.entrySet()) {
            bw.write(dtb.getKey() + "," + dtb.getValue());
            bw.newLine();
        }
        bw.close();
    }

    /**
     * Get the trip distance distribution.
     * @param lines      a list of trip records
     * @return           a map of trip distance distribution, where the KEY is the trip distance, and the VALUE is the count of the distance.
     */
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


/**
 * SparkTripDistribution is a spark program to compute the trip distance distribution.
 */
class SparkTripDistribution {

    /**
     * Given a file recording trip information, write out the trip distance distribution.
     * @param input         path of file containing trip records
     * @param output        path of file to write out the trip distance distribution
     * @throws IOException
     */
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


        //count the occurrence of every trip distance.
        JavaPairRDD<Integer, Integer> distanceDistributionRDD = distancesRDD.reduceByKey(Integer::sum);

        //write the result to file
        writeTripLength(output, distanceDistributionRDD.collect());
        context.stop();
    }

    /**
     * Write the trip distribution to output file.
     * @param output           the output filename
     * @param distribution     a list of (trip distance, count of the distance) tuples.
     * @throws IOException
     */
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

/**
 * DistanceUtilOne is a utility class for exercise one.
 */
class DistanceUtilOne {
    private static final double R = 6371.009;

    /**
     * Given a line of trip record, calculate the trip distance and the trip speed.
     * @param line   A line of trip record
     * @return       A (trip distance, trip speed) tuple
     */
    public static Tuple2<Integer, Double> getDistanceAndSpeedTuple(String line) {
        String[] trip = line.split(" ");
        Double distance = DistanceUtilOne.getSphericalProjectionDistance(
                Double.valueOf(trip[2]), Double.valueOf(trip[3]), Double.valueOf(trip[5]), Double.valueOf(trip[6]));
        Double interval = Double.valueOf(trip[4]) - Double.valueOf(trip[1]);
        Double speed = DistanceUtilOne.getSpeed(distance, interval);
        return new Tuple2<>((int) Math.round(distance), speed);
    }

    /**
     * To get the sphericalProjectionDistance.
     * @param startLat      The latitude of the start position
     * @param startLong     The longtitude of the start position
     * @param endLat        The latitude of the end position
     * @param endLong       The longtitude of the end position
     * @return              Tje computed sphericalProjectionDistance
     */
    public static Double getSphericalProjectionDistance(double startLat, double startLong, double endLat, double endLong) {
        Double deltaLat = (endLat - startLat);
        Double deltaLong = (endLong - startLong);
        Double midLat = (startLat + endLat) / 2 * Math.PI / 180;
        return R * Math.PI * Math.sqrt(Math.pow(deltaLat, 2) + Math.pow(Math.cos(midLat) * deltaLong, 2)) * 1000 / 180;
    }

    /**
     * To get the speed of a trip given distance and trip time.
     * @param distance       The distance of the trip
     * @param tripTime       The time spent on the trip
     * @return               The spped of the trip
     */
    public static Double getSpeed(double distance, double tripTime) {
        double speed = 3.6 * distance / tripTime;
        return speed;
    }
}

