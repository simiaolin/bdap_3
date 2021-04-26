package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.TimeZone;

public class DistanceUtilA {
    static final Double R = 6371.009;
    static final String format = "yyyy-MM-dd HH:mm:ss";    //"yyyy-mm-dd hh:mm:ss" wrong version
    static final TimeZone zone = TimeZone.getTimeZone("America/Los Angeles");
    static SimpleDateFormat sdf = new SimpleDateFormat(format);
    static {
        sdf.setTimeZone(zone);
    }

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


    public static Double getSecondsDouble(String datetime) throws ParseException {
        Date date = sdf.parse(datetime.substring(1, datetime.length()-1));
        return Double.valueOf(date.getTime() / 1000);
    }

    public static long getSecondsLong(String datetime) throws ParseException {
        Date date = sdf.parse(datetime.substring(1, datetime.length()-1));
        long milli = date.getTime();
        return milli / 1000l;
    }

    public static String getDateFromLong(long datetime) {
        Date date = new Date(datetime);
        String formattedDate = sdf.format(date);
        return formattedDate;
    }

    public static LocalDateTime getLocalDatetimeFromDouble(double datetime) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(new Double(datetime).longValue()), zone.toZoneId());
        return localDateTime;
    }



    public static void main(String[] args) throws ParseException {
        long a =  DistanceUtilA.getSecondsLong("'2010-03-01 04:35:13'");
        long b =  DistanceUtilA.getSecondsLong("'2010-07-01 12:00:40'");
        long c =  DistanceUtilA.getSecondsLong("'2010-12-01 12:00:40'");
        System.out.println(a);
        System.out.println(b);
        System.out.println(c);
//  result should be the following
//        1267418113
//        1277985640
//        1291204840
        long bb = 1267418113l * 1000;
        long aa = 1277985640l * 1000;

        System.out.println(getDateFromLong(bb));
        System.out.println(getDateFromLong(aa));

        System.out.println(getSecondsDouble("'2010-03-01 04:35:13'"));
        System.out.println(getSecondsDouble("'2010-07-01 12:00:40'"));
        System.out.println(getSecondsDouble("'2010-12-01 12:00:40'"));
    }

}
