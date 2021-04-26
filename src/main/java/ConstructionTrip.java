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
import utils.DistanceUtilA;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConstructionTrip extends Configured implements Tool {
    private static Logger logger = LoggerFactory.getLogger(ConstructionTrip.class);
    public int run(String[] args) throws Exception  {

        Configuration conf1 = getConf();
        Job job1 = Job.getInstance(conf1, "trip construction");
        job1.setJarByClass(ConstructionTrip.class);
        job1.setMapperClass(SegmentMapper.class);
        job1.setReducerClass(SegmentReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(TimePosTuple.class);
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
//        job2.setInputFormatClass(SequenceFileInputFormat.class);

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


class TimePosTuple implements Writable {
    private Double time;
    private Double latitude;
    private Double longtitude;
    private Boolean isEmpty;

    public TimePosTuple(Double time, Double latitude, Double longtitude, boolean isEmpty) {
        this.time = time;
        this.latitude = latitude;
        this.longtitude = longtitude;
        this.isEmpty = isEmpty;
    }

    public TimePosTuple() {
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
        //todo fix  it
        return 0;
    }
}

class TimePosFullList implements Writable {
    private List<TimePosFull> segmentList;

    public TimePosFullList(List<TimePosFull> segmentList) {
        this.segmentList = segmentList;
    }

    public List<TimePosFull> getSegmentList() {
        return segmentList;
    }

    public void setSegmentList(List<TimePosFull> segmentList) {
        this.segmentList = segmentList;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(segmentList.size());
        for (TimePosFull segment : segmentList) {
            dataOutput.writeDouble(segment.getTime());
            dataOutput.writeDouble(segment.getLatitude());
            dataOutput.writeDouble(segment.getLongtitude());
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        Integer size = dataInput.readInt();
        for (int i = 0; i < size; i++) {
            Double time = dataInput.readDouble();
            Double latitude = dataInput.readDouble();
            Double longtitude = dataInput.readDouble();
            segmentList.add(new TimePosFull(time, latitude, longtitude));
        }
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(segmentList.size());
        for (int i = 0; i < segmentList.size(); i++) {
            str.append("\t");
            str.append(segmentList.get(i).getTime());
            str.append("\t");
            str.append(segmentList.get(i).getLatitude());
            str.append("\t");
            str.append(segmentList.get(i).getLongtitude());
        }
        return str.toString();
    }
}


class SegmentMapper
        extends Mapper<Object, Text, IntWritable, TimePosTuple> {
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String[] segment = value.toString().split(",");
        Integer taxiNum = Integer.valueOf(segment[0]);

        Double startTime = null;
        try {
            startTime = DistanceUtilA.getSecondsDouble(segment[1]);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Double startLat = Double.valueOf(segment[2]);
        Double startLong = Double.valueOf(segment[3]);
        Boolean startStatus = String.valueOf(segment[4]).equals("'E'") ? true : false;

        Double endTime = null;
        try {
            endTime = DistanceUtilA.getSecondsDouble(segment[5]);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Double endLat = Double.valueOf(segment[6]);
        Double endLong = Double.valueOf(segment[7]);
        Boolean endStatus = String.valueOf(segment[8]).equals("'E'") ? true : false;

        context.write(new IntWritable(taxiNum), new TimePosTuple(startTime, startLat, startLong, startStatus));
        context.write(new IntWritable(taxiNum), new TimePosTuple(endTime, endLat, endLong, endStatus));
    }
}


class SegmentReducer
        extends Reducer<IntWritable, TimePosTuple, IntWritable, TimePosFullList> {

    public void reduce(IntWritable key, Iterable<TimePosTuple> values,
                       Context context
    ) throws IOException, InterruptedException {
        List<TimePosTuple> timePosTupleList = new ArrayList<>();
        for (TimePosTuple val : values) {
            timePosTupleList.add(new TimePosTuple(val.getTime(), val.getLatitude(), val.getLongtitude(), val.isEmpty()));
        }

        timePosTupleList.sort((a, b) -> Double.compare(a.getTime(), b.getTime()));
        boolean formerStatus = true;
        List<TimePosFull> currentTrip = new ArrayList<>();

        for (TimePosTuple timePosTuple : timePosTupleList) {
            if (timePosTuple.isEmpty() == false) {  // the car is full
                if (formerStatus == true) {
                    currentTrip.clear();   //the beginning of a new trip
                }
                currentTrip.add(new TimePosFull(timePosTuple.getTime(), timePosTuple.getLatitude(), timePosTuple.getLongtitude()));
                formerStatus = false;
            } else {
                if (formerStatus == false && currentTrip.size() > 1) {        //right after the end of a trip
//                    write(context, currentTrip);
                    context.write(key, new TimePosFullList(currentTrip));
                }

                formerStatus = true;
            }
        }
        if (formerStatus == false && currentTrip.size() > 1) {

            context.write(key, new TimePosFullList(currentTrip));
        }

    }

}

class RevenueMapper extends Mapper<Object, Text, YearAndMonthWritable, DoubleWritable> {
    private YearAndMonthWritable yearAndMonthWritable = new YearAndMonthWritable();
    private DoubleWritable revenueWritable = new DoubleWritable();
    private List<TimePosFull> fullSegmentList = new ArrayList<>();
    public void map(Object key, Text value,
                    Context context
    ) throws IOException, InterruptedException {
        StringTokenizer str = new StringTokenizer(value.toString());
        int size = Integer.valueOf(str.nextToken());
        if (size > 1) {
            for (int i = 0; i < size; i++) {
                double time = Double.valueOf(str.nextToken());
                double latitude = Double.valueOf(str.nextToken());
                double longtitude = Double.valueOf(str.nextToken());
                fullSegmentList.add(new TimePosFull(time, latitude, longtitude));
            }
            if (hasPastByAirport(fullSegmentList)) {
                TimePosFull start = fullSegmentList.get(0);
                TimePosFull end = fullSegmentList.get(size - 1);
                if (isRouteReasonable(start, end)) {
                    double revenue = getRevenueFromPos(start, end);
                    LocalDateTime localDateTime  = DistanceUtilA.getLocalDatetimeFromDouble(start.getTime());
                    yearAndMonthWritable.setYear(localDateTime.getYear());
                    yearAndMonthWritable.setMonth(localDateTime.getMonthValue());
                    revenueWritable.set(revenue);
                    context.write(yearAndMonthWritable, revenueWritable);
                }
            }
        }
    }

    //todo
    public static boolean hasPastByAirport(List<TimePosFull> fullSegmentList) {
        return true;
    }

    //todo
    public static boolean isRouteReasonable(TimePosFull start, TimePosFull end) {
        return true;
    }

    public static double getRevenueFromPos(TimePosFull start, TimePosFull end) {
        double startLat = start.getLatitude();
        double startLong = start.getLongtitude();
        double endLat = end.getLatitude();
        double endLong = end.getLongtitude();
        double distance = DistanceUtilA.getSphericalProjectionDistance(startLat, startLong, endLat, endLong);
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

