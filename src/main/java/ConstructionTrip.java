import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.DistanceUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConstructionTrip {
    private static Logger logger = LoggerFactory.getLogger(ConstructionTrip.class);

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "trip construction");
        job.setJarByClass(ConstructionTrip.class);

        job.setMapperClass(SegmentMapper.class);
//        job.setCombinerClass(SegmentReducer.class);
        job.setReducerClass(SegmentReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(TimePosTuple.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        long end = System.currentTimeMillis();
        logger.info("spend time " + (end - start));
        System.out.println("spend time " + (end - start));
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
        StringBuilder str = new StringBuilder(segmentList.size());
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
            startTime = DistanceUtil.getSecondsDouble(segment[1]);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Double startLat = Double.valueOf(segment[2]);
        Double startLong = Double.valueOf(segment[3]);
        Boolean startStatus = String.valueOf(segment[4]).equals("'E'") ? true : false;

        Double endTime = null;
        try {
            endTime = DistanceUtil.getSecondsDouble(segment[5]);
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
                if (formerStatus == false) {        //right after the end of a trip
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
