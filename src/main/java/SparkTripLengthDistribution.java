import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkTripLengthDistribution {
    static final Double R = 6371.009;
    private static Logger logger=LoggerFactory.getLogger(SparkTripLengthDistribution.class);

    public static class DistanceMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private IntWritable distanceIntWritable = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String taxiNum = itr.nextToken();
            Double beginTime = Double.valueOf(itr.nextToken());
            Double startLat = Double.valueOf(itr.nextToken());
            Double startLong = Double.valueOf(itr.nextToken());
            Double endTime = Double.valueOf(itr.nextToken());
            Double endLat = Double.valueOf(itr.nextToken());
            Double endLong = Double.valueOf(itr.nextToken());
            int distance = (int)Math.round(getSphericalProjectionDistance(startLat, startLong, endLat, endLong));
            Double tripTime = endTime - beginTime;
            Double speed = 3.6 * distance / tripTime;
            distanceIntWritable.set(distance);
            if (speed < 300) {
                context.write(distanceIntWritable, one);
            }
        }
    }


  public static class IntSumReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static Double getSphericalProjectionDistance(double startLat, double startLong, double endLat, double endLong) {
        Double deltaLat = (endLat - startLat) ;
        Double deltaLong = (endLong - startLong) ;
        Double midLat = (startLat + endLat) / 2 * Math.PI / 180;
        return R * Math.PI * Math.sqrt(Math.pow(deltaLat, 2) + Math.pow(Math.cos(midLat) * deltaLong, 2)) * 1000 / 180;
    }


    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "trip length");
        job.setJarByClass(SparkTripLengthDistribution.class);

        job.setMapperClass(DistanceMapper.class);

        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        long end = System.currentTimeMillis();
        logger.info("spend time " + (end - start) );
        System.out.println("spend time " + (end - start) );
    }
}
