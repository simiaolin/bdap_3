import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class TripLengthDistribution {
    static final Double R = 6371.009;

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

    public static void writeTripLength(String output, List<Double>  distances) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(output));
        for (Double dist : distances) {
            bw.write(String.valueOf(dist));
            bw.newLine();
        }
        bw.close();
    }

    public static List<Double> getDistances(List<String> lines) {
        List<Double> distances = new ArrayList<>();
        for (String line : lines) {
            String[] columns = line.split(" ");
            Double startLat = Double.valueOf(columns[2]);
            Double startLong = Double.valueOf(columns[3]);
            Double endLat = Double.valueOf(columns[5]);
            Double endLong = Double.valueOf(columns[6]);
            distances.add(getDistance(startLat, startLong, endLat, endLong));
        }
        return distances;
    }

    //todo investigate other two
    public static Double getDistance(double startLat, double startLong, double endLat, double endLong) {
        Double startColatitudeInRadian = getColatitudeInRadian(startLat);
        Double endColatitudeInRadian = getColatitudeInRadian(endLat);
        Double deltaLong = endLong - startLong;
        return R * Math.sqrt(Math.pow(startColatitudeInRadian, 2) + Math.pow(endColatitudeInRadian, 2) - 2 * startColatitudeInRadian * endColatitudeInRadian * Math.cos(deltaLong));
    }

    public static Double getColatitudeInRadian(Double degree) {
        return Math.PI * (90 - degree) / 180;
    }


    public static void main(String[] args) throws IOException {
        List<String> lines = readTripFile(args[0]);
        List<Double> distances = getDistances(lines);
        System.out.println(distances.size());

        writeTripLength(args[1], distances);

    }
}
