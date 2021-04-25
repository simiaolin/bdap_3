package example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.IOException;
import java.nio.ByteBuffer;

public class WordCombined extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        JobControl jobControl = new JobControl("jobChain");
        Configuration conf1 = getConf();

        Job job1 = Job.getInstance(conf1);
        job1.setJarByClass(WordCombined.class);
        job1.setJobName("Word Combined");

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));

        job1.setMapperClass(WordMapper.class);
        job1.setReducerClass(SumReducer.class);
        job1.setCombinerClass(SumReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        ControlledJob controlledJob1 = new ControlledJob(conf1);
        controlledJob1.setJob(job1);

        jobControl.addJob(controlledJob1);
        Configuration conf2 = getConf();

        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(WordCombined.class);
        job2.setJobName("Word Invert");

        FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

        job2.setMapperClass(WordMapper2.class);
        job2.setReducerClass(SumReducer2.class);
        job2.setCombinerClass(SumReducer2.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
//        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);

        ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.setJob(job2);

        // make job2 dependent on job1
        controlledJob2.addDependingJob(controlledJob1);
        // add the job to the job control
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
        int exitCode = ToolRunner.run(new WordCombined(), args);
        System.exit(exitCode);
    }
}


class IntComparator extends WritableComparator {

    public IntComparator() {
        super(IntWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2,
                       int s2, int l2) {
        Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
        Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();
        return v1.compareTo(v2) * (-1);
    }
}

class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    IntWritable intWritable = new IntWritable(1);
    Text text = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        for (String word : line.split("\\W+")) {
            if (word.length() > 0) {
                text.set(word);
                context.write(text, intWritable);
            }
        }
    }
}


class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable intWritable = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int wordCount = 0;
        for (IntWritable value : values) {
            wordCount += value.get();
        }
        intWritable.set(wordCount);
        context.write(key, intWritable);
    }
}

class WordMapper2 extends Mapper<Text, Text, IntWritable, Text> {

    IntWritable frequency = new IntWritable();

    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        int newVal = Integer.parseInt(value.toString());
        frequency.set(newVal);
        context.write(frequency, key);
    }
}

class SumReducer2 extends Reducer<IntWritable, Text, IntWritable, Text> {

    Text word = new Text();

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            word.set(value);
            context.write(key, word);
        }
    }
}