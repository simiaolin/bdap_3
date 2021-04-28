import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConstructionTrip extends Configured implements Tool {
    private static Logger logger = LoggerFactory.getLogger(ConstructionTrip.class);

    public int run(String[] args) throws Exception {

        Configuration conf1 = getConf();
        Job job1 = Job.getInstance(conf1, "trip construction");
        job1.setJarByClass(ConstructionTrip.class);
        job1.setMapperClass(SegmentMapper.class);
        job1.setReducerClass(SegmentReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(TimePosTupleWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));


        Configuration conf2 = getConf();
        Job job2 = Job.getInstance(conf2, "revenue distribution");
        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.setMapperClass(RevenueMapper.class);
        job2.setReducerClass(RevenueReducer.class);
        job2.setOutputKeyClass(YearAndMonthWritable.class);
        job2.setOutputValueClass(DoubleWritable.class);

        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        int splitSize = Integer.valueOf(args[3]);
        int reduceTaskNum = Integer.valueOf(args[4]);
//        job2.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.setMaxInputSplitSize(job1, splitSize);
        FileInputFormat.setMaxInputSplitSize(job2, splitSize);
        job1.setNumReduceTasks(reduceTaskNum);
        job2.setNumReduceTasks(reduceTaskNum);
        JobControl jobControl = new JobControl("job chain");
        ControlledJob controlledJob1 = new ControlledJob(conf1);
        controlledJob1.setJob(job1);
        jobControl.addJob(controlledJob1);

        ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.setJob(job2);
        controlledJob2.addDependingJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();

        while (!jobControl.allFinished()) {
            System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());
            System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
            System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
            System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
            System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
            try {
                Thread.sleep(5000);
            } catch (Exception e) {

            }

        }
        System.exit(0);
        return (job1.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ConstructionTrip(), args);
        System.exit(exitCode);
    }


}


class TimePosTupleWritable implements Writable {
    private Double time;
    private Double latitude;
    private Double longtitude;
    private Boolean isEmpty;

    public TimePosTupleWritable(Double time, Double latitude, Double longtitude, boolean isEmpty) {
        this.time = time;
        this.latitude = latitude;
        this.longtitude = longtitude;
        this.isEmpty = isEmpty;
    }

    public TimePosTupleWritable() {
    }

    public Double getTime() {
        return time;
    }

    public void setTime(Double time) {
        this.time = time;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongtitude() {
        return longtitude;
    }

    public void setLongtitude(Double longtitude) {
        this.longtitude = longtitude;
    }

    public boolean isEmpty() {
        return isEmpty;
    }

    public void setEmpty(boolean empty) {
        isEmpty = empty;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(time);
        dataOutput.writeDouble(latitude);
        dataOutput.writeDouble(longtitude);
        dataOutput.writeBoolean(isEmpty);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        time = dataInput.readDouble();
        latitude = dataInput.readDouble();
        longtitude = dataInput.readDouble();
        isEmpty = dataInput.readBoolean();
    }

    @Override
    public String toString() {
        return time + "\t" + latitude + "\t" + longtitude + "\t" + isEmpty;
    }
}

class TimePosFull {
    private Double time;
    private Double latitude;
    private Double longtitude;

    public TimePosFull() {

    }

    public TimePosFull(Double time, Double latitude, Double longtitude) {
        this.time = time;
        this.latitude = latitude;
        this.longtitude = longtitude;
    }

    public Double getTime() {
        return time;
    }

    public void setTime(Double time) {
        this.time = time;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongtitude() {
        return longtitude;
    }

    public void setLongtitude(Double longtitude) {
        this.longtitude = longtitude;
    }
}

class YearAndMonthWritable implements WritableComparable<YearAndMonthWritable> {
    private Integer year;
    private Integer month;

    public YearAndMonthWritable() {

    }

    public YearAndMonthWritable(Integer year, Integer month) {
        this.year = year;
        this.month = month;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public Integer getMonth() {
        return month;
    }

    public void setMonth(Integer month) {
        this.month = month;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(month);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readInt();
        month = dataInput.readInt();
    }

    //Todo:
    @Override
    public String toString() {
        return this.getYear() + "\t" + this.getMonth();
    }

    @Override
    public int compareTo(YearAndMonthWritable o) {
        if (year > o.getYear()) {
            return 1;
        } else if (year < o.getYear()) {
            return -1;
        } else {
            return Integer.compare(month, o.getMonth());
        }
    }
}

class TripWritable implements Writable {
    private boolean hasPassByAirport;
    private TimePosFull start;
    private TimePosFull end;
    private Double distance;

    public TripWritable() {
    }

    public TripWritable(boolean hasPassByAirport, TimePosFull start, TimePosFull end, Double distance) {
        this.hasPassByAirport = hasPassByAirport;
        this.start = start;
        this.end = end;
        this.distance = distance;
    }

    public boolean isHasPassByAirport() {
        return hasPassByAirport;
    }

    public void setHasPassByAirport(boolean hasPassByAirport) {
        this.hasPassByAirport = hasPassByAirport;
    }

    public TimePosFull getStart() {
        return start;
    }

    public void setStart(TimePosFull start) {
        this.start = start;
    }

    public TimePosFull getEnd() {
        return end;
    }

    public void setEnd(TimePosFull end) {
        this.end = end;
    }

    public Double getDistance() {
        return distance;
    }

    public void setDistance(Double distance) {
        this.distance = distance;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(isHasPassByAirport());
        write(dataOutput, start);
        write(dataOutput, end);
        dataOutput.writeDouble(distance);
    }

    public void write(DataOutput dataOutput, TimePosFull timePosFull) throws IOException {
        dataOutput.writeDouble(timePosFull.getTime());
        dataOutput.writeDouble(timePosFull.getLatitude());
        dataOutput.writeDouble(timePosFull.getLongtitude());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        hasPassByAirport = dataInput.readBoolean();

        start.setTime(dataInput.readDouble());
        start.setLatitude(dataInput.readDouble());
        start.setLongtitude(dataInput.readDouble());
        end.setTime(dataInput.readDouble());
        end.setLatitude(dataInput.readDouble());
        end.setLongtitude(dataInput.readDouble());
        distance = dataInput.readDouble();

    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(hasPassByAirport);
        str.append(toString(start));
        str.append(toString(end));
        str.append("\t");
        str.append(distance);
        return str.toString();
    }

    public String toString(TimePosFull timePosFull) {
        StringBuilder str = new StringBuilder();
        str.append("\t");
        str.append(timePosFull.getTime());
        str.append("\t");
        str.append(timePosFull.getLatitude());
        str.append("\t");
        str.append(timePosFull.getLongtitude());
        return str.toString();
    }
}


class SegmentMapper
        extends Mapper<Object, Text, IntWritable, TimePosTupleWritable> {
    private IntWritable taxiNumWritable = new IntWritable();
    private TimePosTupleWritable timePosTupleWritable = new TimePosTupleWritable(0.0, 0.0, 0.0, false);

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String[] segment = value.toString().split(",");
        try {
            Integer taxiNum = Integer.valueOf(segment[0]);
            Double startTime = DistanceUtil.getSecondsDouble(segment[1]);
            Double startLat = Double.valueOf(segment[2]);
            Double startLong = Double.valueOf(segment[3]);
            Boolean startStatus = String.valueOf(segment[4]).equals("'E'") ? true : false;

            Double endTime = DistanceUtil.getSecondsDouble(segment[5]);
            Double endLat = Double.valueOf(segment[6]);
            Double endLong = Double.valueOf(segment[7]);
            Boolean endStatus = String.valueOf(segment[8]).equals("'E'") ? true : false;
            taxiNumWritable.set(taxiNum);

            timePosTupleWritable.setEmpty(startStatus);
            timePosTupleWritable.setTime(startTime);
            timePosTupleWritable.setLatitude(startLat);
            timePosTupleWritable.setLongtitude(startLong);
            context.write(taxiNumWritable, timePosTupleWritable);

            timePosTupleWritable.setEmpty(endStatus);
            timePosTupleWritable.setTime(endTime);
            timePosTupleWritable.setLatitude(endLat);
            timePosTupleWritable.setLongtitude(endLong);
            context.write(taxiNumWritable, timePosTupleWritable);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}


class SegmentReducer
        extends Reducer<IntWritable, TimePosTupleWritable, IntWritable, TripWritable> {
    private List<TimePosTupleWritable> timePosTupleWritableList = new ArrayList();
    private TimePosFull start = new TimePosFull();
    private TimePosFull end = new TimePosFull();
    private static final double airPortLat = 37.62131;
    private static final double airPortLong = -122.37896;

    public void reduce(IntWritable key, Iterable<TimePosTupleWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        timePosTupleWritableList.clear();
        for (TimePosTupleWritable val : values) {
            timePosTupleWritableList.add(new TimePosTupleWritable(val.getTime(), val.getLatitude(), val.getLongtitude(), val.isEmpty()));
        }
        timePosTupleWritableList.sort((a, b) -> Double.compare(a.getTime(), b.getTime()));
        boolean formerStatus = true;
        int firstFullIndex = 0;
        int lastFullIndex = 0;
        boolean hasPastByAirport = false;
        for (int i = 0; i < timePosTupleWritableList.size(); i++) {
            if (timePosTupleWritableList.get(i).isEmpty() == false) { // the car is full
                if (formerStatus == true) {
                    //the beginning of a new trip
                    hasPastByAirport = false;
                    firstFullIndex = i;
                } else {
                    lastFullIndex = i;
                }
                formerStatus = false;
                if (checkIfPastByAirport(timePosTupleWritableList.get(i))) {
                    hasPastByAirport = true;
                }
            } else {
                if (formerStatus == false && lastFullIndex > firstFullIndex) {   //the car just becomes empty
                    writeContext(context, firstFullIndex, lastFullIndex, key, hasPastByAirport);
                }
                formerStatus = true;
            }
        }
        if (formerStatus == false && lastFullIndex > firstFullIndex) {
            writeContext(context, firstFullIndex, lastFullIndex, key,hasPastByAirport);

        }
    }

    public void writeContext(Context context, int firstFullIndex, int lastFullIndex, IntWritable key, boolean hasPastByAirport) throws IOException, InterruptedException {
        start.setTime(timePosTupleWritableList.get(firstFullIndex).getTime());
        start.setLatitude(timePosTupleWritableList.get(firstFullIndex).getLatitude());
        start.setLongtitude(timePosTupleWritableList.get(firstFullIndex).getLongtitude());
        end.setTime(timePosTupleWritableList.get(lastFullIndex).getTime());
        end.setLatitude(timePosTupleWritableList.get(lastFullIndex).getLatitude());
        end.setLongtitude(timePosTupleWritableList.get(lastFullIndex).getLongtitude());

        //check whether the trip is reasonable
        double interval = end.getTime() - start.getTime();
        double distance = DistanceUtil.getSphericalProjectionDistance(start, end);
        double speed = DistanceUtil.getSpeed(distance, interval);
        boolean isRouteReasonable =  speed > 0.0 && speed < 200.0;
        if (isRouteReasonable) {
            context.write(key, new TripWritable(hasPastByAirport, start, end, distance));
        }
    }


    public static boolean checkIfPastByAirport(TimePosTupleWritable timePos) {
        if (DistanceUtil.getSphericalProjectionDistance(timePos.getLatitude(), timePos.getLongtitude(), airPortLat, airPortLong) <= 1000.0) {
            return true;
        } else {
            return false;
        }
    }

}

class RevenueMapper extends Mapper<Object, Text, YearAndMonthWritable, DoubleWritable> {
    private YearAndMonthWritable yearAndMonthWritable = new YearAndMonthWritable();
    private DoubleWritable revenueWritable = new DoubleWritable();


    public void map(Object key, Text value,
                    Context context
    ) throws IOException, InterruptedException {
        String[] trip = value.toString().split("\t");
        boolean hasPassByAirport = Boolean.valueOf(trip[0]);
        Double distance = Double.valueOf(trip[7]);
        Double startTime = Double.valueOf(trip[1]);
        if (hasPassByAirport) {
            double revenue = getRevenueFromDistance(distance);
            LocalDateTime localDateTime = DistanceUtil.getLocalDatetimeFromDouble(startTime);
            yearAndMonthWritable.setYear(localDateTime.getYear());
            yearAndMonthWritable.setMonth(localDateTime.getMonthValue());
            revenueWritable.set(revenue);
            context.write(yearAndMonthWritable, revenueWritable);
        }
    }


    public static double getRevenueFromPos(TimePosFull start, TimePosFull end) {
        double distance = DistanceUtil.getSphericalProjectionDistance(start,end);
        return getRevenueFromDistance(distance);
    }


    public static double getRevenueFromDistance(double distance) {
        return 3.5 + distance * 1.71 / 1000;
    }

}


class RevenueReducer extends Reducer<YearAndMonthWritable, DoubleWritable, YearAndMonthWritable, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(YearAndMonthWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}

class DistanceUtil {
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

    public static Double getSphericalProjectionDistance(TimePosFull start, TimePosFull end) {
        double startLat = start.getLatitude();
        double startLong = start.getLongtitude();
        double endLat = end.getLatitude();
        double endLong = end.getLongtitude();
        double distance = DistanceUtil.getSphericalProjectionDistance(startLat, startLong, endLat, endLong);
        return distance;
    }

    public static Double getSphericalProjectionDistance(double startLat, double startLong, double endLat, double endLong) {
        Double deltaLat = (endLat - startLat);
        Double deltaLong = (endLong - startLong);
        Double midLat = (startLat + endLat) / 2 * Math.PI / 180;
        return R * Math.PI * Math.sqrt(Math.pow(deltaLat, 2) + Math.pow(Math.cos(midLat) * deltaLong, 2)) * 1000 / 180;
    }

    public static Double getColatitudeInRadian(Double degree) {
        return Math.PI * (90 - degree) / 180;
    }

   public static Double getSpeed(double distance, double tripTime) {
       double speed = 3.6 * distance / tripTime;
       return speed;
   }

    public static Double getSecondsDouble(String datetime) throws ParseException {
        Date date = sdf.parse(datetime.substring(1, datetime.length() - 1));
        return Double.valueOf(date.getTime() / 1000);
    }

    public static long getSecondsLong(String datetime) throws ParseException {
        Date date = sdf.parse(datetime.substring(1, datetime.length() - 1));
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
}
