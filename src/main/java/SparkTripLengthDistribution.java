import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class SparkTripLengthDistribution {

    public static class DistanceMapper
            extends Mapper<Object, Text, DoubleWritable, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

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
            Double distance = DistanceUtil.getSphericalProjectionDistance(startLat, startLong, endLat, endLong);
            Double tripTime = endTime - beginTime;
            Double speed = 3.6 * distance / tripTime;
            if (speed > 0 && speed < 300) {
                context.write(new DoubleWritable(distance), one);
            }
        }
    }


    public static class MinMaxMapper
            extends Reducer<DoubleWritable, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
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

  public static class IntSumReducer
            extends Reducer<DoubleWritable, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
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
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "trip length");
        job.setJarByClass(SparkTripLengthDistribution.class);

        job.setMapperClass(DistanceMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
