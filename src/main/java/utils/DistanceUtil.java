package utils;

public class DistanceUtil {
    static final Double R = 6371.009;

    //todo investigate other two
    public static Double getPolarCoordinateDistance(double startLat, double startLong, double endLat, double endLong) {
        Double startColatitudeInRadian = getColatitudeInRadian(startLat);
        Double endColatitudeInRadian = getColatitudeInRadian(endLat);
        Double deltaLong = (endLong - startLong) * Math.PI / 180;
        return R * Math.sqrt(Math.pow(startColatitudeInRadian, 2) + Math.pow(endColatitudeInRadian, 2)
                - 2 * startColatitudeInRadian * endColatitudeInRadian * Math.cos(deltaLong)) * 1000;
    }

    public static Double getSphericalProjectionDistance(double startLat, double startLong, double endLat, double endLong) {
        Double deltaLat = (endLat - startLat) ;
        Double deltaLong = (endLong - startLong) ;
        Double midLat = (startLat + endLat) / 2 * Math.PI / 180;
        return R * Math.PI * Math.sqrt(Math.pow(deltaLat, 2) + Math.pow(Math.cos(midLat) * deltaLong, 2)) * 1000 / 180;
    }

    public static Double getColatitudeInRadian(Double degree) {
        return Math.PI * (90 - degree) / 180;
    }


    public static Double getSystemMillis(String datetime) {
        return 0.0;
    }
}
