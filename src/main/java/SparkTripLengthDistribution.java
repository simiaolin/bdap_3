import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.DistanceUtil;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.log4j.pattern.LogEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkTripLengthDistribution {
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
            int distance = (int)Math.round(DistanceUtil.getSphericalProjectionDistance(startLat, startLong, endLat, endLong));
            Double tripTime = endTime - beginTime;
            Double speed = 3.6 * distance / tripTime;
            distanceIntWritable.set(distance);
            if (speed > 0 && speed < 300) {
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

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "trip length");
        job.setJarByClass(SparkTripLengthDistribution.class);


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
