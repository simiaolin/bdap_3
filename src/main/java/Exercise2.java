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

public class Exercise2 extends Configured implements Tool {
    private static Logger logger = LoggerFactory.getLogger(Exercise2.class);

    public int run(String[] args) throws Exception {
        if (args.length < 9) {
            throw new Exception("please specify 9 arguments! \n" +
                    "[1]: input file path \n" +
                    "[2]: output file path for trip reconstruction \n" +
                    "[3]: output file path for airport revenue distribution \n" +
                    "[4]: split size of mapper on job 1 \n" +
                    "[5]: task num of reducer on job 1 \n" +
                    "[6]: split size of mapper on job 2 \n" +
                    "[7]: task num of reducer on job 2 \n" +
                    "[8]: job name of job 1 \n" +
                    "[9]: job name of job 2\n");
        }

        //basic configuration for job 1
        Configuration conf1 = getConf();
        String nameOfJob1 = args[7];
        String nameOfJob2 = args[8];
        Job job1 = Job.getInstance(conf1, nameOfJob1);
        job1.setJarByClass(Exercise2.class);
        job1.setMapperClass(SegmentMapper.class);
        job1.setReducerClass(SegmentReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(TimePosTupleWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        //basic configuration for job 2
        Configuration conf2 = getConf();
        Job job2 = Job.getInstance(conf2, nameOfJob2);
        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.setJarByClass(Exercise2.class);
        job2.setMapperClass(RevenueMapper.class);
        job2.setCombinerClass(RevenueReducer.class);
        job2.setReducerClass(RevenueReducer.class);
        job2.setOutputKeyClass(YearAndMonthWritable.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        //configuration on size of mappers and reducers on jobs
        int splitSizeOfJob1 = Integer.valueOf(args[3]);
        int reduceTaskNumOfJob1 = Integer.valueOf(args[4]);
        int splitSizeOfJob2 = Integer.valueOf(args[5]);
        int reduceTaskNumOfJob2 = Integer.valueOf(args[6]);
        FileInputFormat.setMaxInputSplitSize(job1, splitSizeOfJob1);
        FileInputFormat.setMaxInputSplitSize(job2, splitSizeOfJob2);
        job1.setNumReduceTasks(reduceTaskNumOfJob1);
        job2.setNumReduceTasks(reduceTaskNumOfJob2);

        //build a JobControl, chaining job 1 and job 2 together
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
        int exitCode = ToolRunner.run(new Exercise2(), args);
        System.exit(exitCode);
    }
}

//Map the segment record to taxi number.
//For each line in the input, there are two segments, retrieve these info and write them into the corresponding taxi.
class SegmentMapper
        extends Mapper<Object, Text, IntWritable, TimePosTupleWritable> {
    private IntWritable taxiNumWritable = new IntWritable();
    private TimePosTupleWritable timePosTupleWritable = new TimePosTupleWritable(0.0, 0.0, 0.0, false);

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String[] segment = value.toString().split(",");
        try {
            Integer taxiNum = Integer.valueOf(segment[0]);
            Double startTime = DistanceUtilTwo.getSecondsDouble(segment[1]);
            Double startLat = Double.valueOf(segment[2]);
            Double startLong = Double.valueOf(segment[3]);
            Boolean startStatus = String.valueOf(segment[4]).equals("'E'") ? true : false;

            timePosTupleWritable.setEmpty(startStatus);
            timePosTupleWritable.setTime(startTime);
            timePosTupleWritable.setLatitude(startLat);
            timePosTupleWritable.setLongtitude(startLong);
            taxiNumWritable.set(taxiNum);
            context.write(taxiNumWritable, timePosTupleWritable);     //information of first segment
        } catch (ParseException e) {
            e.printStackTrace();                                      //in case there is mis-formatted data
        }

        try {
            Integer taxiNum = Integer.valueOf(segment[0]);

            Double endTime = DistanceUtilTwo.getSecondsDouble(segment[5]);
            Double endLat = Double.valueOf(segment[6]);
            Double endLong = Double.valueOf(segment[7]);
            Boolean endStatus = String.valueOf(segment[8]).equals("'E'") ? true : false;
            taxiNumWritable.set(taxiNum);

            timePosTupleWritable.setEmpty(endStatus);
            timePosTupleWritable.setTime(endTime);
            timePosTupleWritable.setLatitude(endLat);
            timePosTupleWritable.setLongtitude(endLong);
            context.write(taxiNumWritable, timePosTupleWritable);   //information of second segment
        } catch (ParseException e) {
            e.printStackTrace();                                    //in case there is mis-formatted data
        }
    }
}

//For each taxi, first sort its segments according to its time info, then retrieve consecutive segments with status 'M', which means the taxi is full.
//For example, if we have 10 consecutive sorted segments like 1_E, 2_E, 3_M, 4_M, 5_M, 6_E, 7_E, 8_M, 9_M, 10_E, we can retrieve two trips,
// <3_M, 4_M, 5_M>  and <8_M, 9_M> , respectively.
//For each retrieved trip, calculate the distance, filter on its speed, check whether it has past the airport.
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
        boolean isFormerSegmentEmpty = true;
        int currentStartSegmentIndex = 0;    //index of segment that is the begin of a trip
        int currentEndSegmentIndex = 0;     //index of segment that is the end of a trip
        double currentDistance = 0;
        boolean hasPastByAirport = false;
        for (int i = 0; i < timePosTupleWritableList.size(); i++) {
            if (timePosTupleWritableList.get(i).isEmpty() == false) { // the car is full
                if (isFormerSegmentEmpty == true) {                     //indicates the beginning of a new trip
                    hasPastByAirport = false;
                    currentStartSegmentIndex = i;
                    currentDistance = 0;                        //reset the current distance
                } else {
                    currentEndSegmentIndex = i;
                    double formerLatitude = timePosTupleWritableList.get(i - 1).getLatitude();
                    double formerLongtitude = timePosTupleWritableList.get(i - 1).getLongtitude();
                    double currentLatitude = timePosTupleWritableList.get(i).getLatitude();
                    double currentLongtitude = timePosTupleWritableList.get(i).getLongtitude();
                    currentDistance += DistanceUtilTwo.getSphericalProjectionDistance(formerLatitude, formerLongtitude, currentLatitude, currentLongtitude);
                }
                isFormerSegmentEmpty = false;
                if (checkIfPastByAirport(timePosTupleWritableList.get(i))) {
                    hasPastByAirport = true;
                }
            } else {
                if (isFormerSegmentEmpty == false && currentEndSegmentIndex > currentStartSegmentIndex) {   //the car just becomes empty again
                    writeContext(context, key, currentStartSegmentIndex, currentEndSegmentIndex, hasPastByAirport, currentDistance);                  //write out the current reconstructed trip.
                }
                isFormerSegmentEmpty = true;
            }
        }
        if (isFormerSegmentEmpty == false && currentEndSegmentIndex > currentStartSegmentIndex) {             // the last few segments in the timePosTupleWritableList form a trip
            writeContext(context, key, currentStartSegmentIndex, currentEndSegmentIndex, hasPastByAirport, currentDistance);

        }
    }

    /**
     * Write out the constructed trips.
     * @param context                    the spark context
     * @param taxiId                     the taxi ID
     * @param startSegmentIndex          the index of the start segment in the timePosTupleWritableList
     * @param endSegmentIndex            the index of the end segment in the timePosTupleWritableList
     * @param hasPastByAirport           whether the current trip has passed by the airport
     * @param distance                   the distance of the current trip,
     *                                   which is the sum of distances between consecutive segments in the middle of the start segment and the end segment
     * @throws IOException
     * @throws InterruptedException
     */
    public void writeContext(Context context, IntWritable taxiId, int startSegmentIndex, int endSegmentIndex, boolean hasPastByAirport, double distance) throws IOException, InterruptedException {
        start.setTime(timePosTupleWritableList.get(startSegmentIndex).getTime());
        start.setLatitude(timePosTupleWritableList.get(startSegmentIndex).getLatitude());
        start.setLongtitude(timePosTupleWritableList.get(startSegmentIndex).getLongtitude());
        end.setTime(timePosTupleWritableList.get(endSegmentIndex).getTime());
        end.setLatitude(timePosTupleWritableList.get(endSegmentIndex).getLatitude());
        end.setLongtitude(timePosTupleWritableList.get(endSegmentIndex).getLongtitude());

        //check whether the trip is reasonable
        double interval = end.getTime() - start.getTime();
        double speed = DistanceUtilTwo.getSpeed(distance, interval);
        boolean isRouteReasonable =  speed > 0.0 && speed < 200.0;
        if (isRouteReasonable) {
            context.write(taxiId, new TripWritable(hasPastByAirport, start, end, distance));
        }
    }


    public static boolean checkIfPastByAirport(TimePosTupleWritable timePos) {
        if (DistanceUtilTwo.getSphericalProjectionDistance(timePos.getLatitude(), timePos.getLongtitude(), airPortLat, airPortLong) <= 1000.0) {
            return true;
        } else {
            return false;
        }
    }

}

//For each input, if it is a trip passing through the airport, calculate its revenue and map the revenue to its time
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
        if (hasPassByAirport) {            //only deals with the trip that passes through the airport
            double revenue = getRevenueFromDistance(distance);
            LocalDateTime localDateTime = DistanceUtilTwo.getLocalDatetimeFromDouble(startTime);
            yearAndMonthWritable.setYear(localDateTime.getYear());
            yearAndMonthWritable.setMonth(localDateTime.getMonthValue());
            revenueWritable.set(revenue);
            context.write(yearAndMonthWritable, revenueWritable);
        }
    }

    public static double getRevenueFromDistance(double distance) {
        return 3.5 + distance * 1.71 / 1000;
    }

}

//Sum over the revenue for each month.
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

/**
 * DistanceUtilTwo is a utility class for exercise two.
 */
class DistanceUtilTwo {
    static final Double R = 6371.009;
    static final String format = "yyyy-MM-dd HH:mm:ss";
    static final TimeZone zone = TimeZone.getTimeZone("America/Los Angeles");
    static SimpleDateFormat sdf = new SimpleDateFormat(format);

    static {
        sdf.setTimeZone(zone);
    }

    //Get spherical projection distance provided start point and end point.
    public static Double getSphericalProjectionDistance(TimePosFull start, TimePosFull end) {
        double startLat = start.getLatitude();
        double startLong = start.getLongtitude();
        double endLat = end.getLatitude();
        double endLong = end.getLongtitude();
        double distance = DistanceUtilTwo.getSphericalProjectionDistance(startLat, startLong, endLat, endLong);
        return distance;
    }

    //Get spherical projection distance provided the latitudes and longtitudes of start point and end point.
    public static Double getSphericalProjectionDistance(double startLat, double startLong, double endLat, double endLong) {
        Double deltaLat = (endLat - startLat);
        Double deltaLong = (endLong - startLong);
        Double midLat = (startLat + endLat) / 2 * Math.PI / 180;
        return R * Math.PI * Math.sqrt(Math.pow(deltaLat, 2) + Math.pow(Math.cos(midLat) * deltaLong, 2)) * 1000 / 180;
    }

    //Get the trip speed given trip distance and trip time.
   public static Double getSpeed(double distance, double tripTime) {
       double speed = 3.6 * distance / tripTime;
       return speed;
   }

    //Transform datetime from string form to double form.
    public static Double getSecondsDouble(String datetime) throws ParseException {
        Date date = sdf.parse(datetime.substring(1, datetime.length() - 1));
        return Double.valueOf(date.getTime() / 1000);
    }

    //Get datetime from its double form.
    public static LocalDateTime getLocalDatetimeFromDouble(double datetime) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(new Double(datetime).longValue()), zone.toZoneId());
        return localDateTime;
    }
}

//TimePosTupleWritable records the info of a single segment
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

//YearAndMonthWritable records the year and month information
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
        return this.getYear() + "_" + this.getMonth();
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

    @Override
    public boolean equals(Object o) {
        if (o instanceof YearAndMonthWritable) {
            return year.equals(((YearAndMonthWritable) o).getYear()) && month.equals(((YearAndMonthWritable) o).getMonth());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 31 * year.hashCode() + month.hashCode();
    }
}

//TripWritable records the reconstructed trip information.
//It is in the output of first job as well as the input of the second job.
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

//TimePosFull is a segment with status 'M'(full)
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
