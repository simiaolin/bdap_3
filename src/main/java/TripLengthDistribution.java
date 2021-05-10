import utils.DistanceUtilA;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TripLengthDistribution {

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


    public static HashMap<Integer, Integer> getDistances(List<String> lines) {
        HashMap<Integer, Integer> distribution = new HashMap<>();
        for (String line : lines) {
            String[] columns = line.split(" ");
            Double startLat = Double.valueOf(columns[2]);
            Double startLong = Double.valueOf(columns[3]);
            Double endLat = Double.valueOf(columns[5]);
            Double endLong = Double.valueOf(columns[6]);
            Integer distance = (int) Math.round(DistanceUtilA.getSphericalProjectionDistance(startLat, startLong, endLat, endLong));
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

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        List<String> lines = readTripFile(args[0]);
        HashMap<Integer, Integer> distribution = getDistances(lines);
        writeTripLength(args[1], distribution);
        long end = System.currentTimeMillis();
        System.out.println(end - start);

    }

}

