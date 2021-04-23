package example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

import org.apache.commons.configuration.ConfigurationFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MinMaxExample {
    public static void main(String[] args) throws Exception {
        /* delete the output directory before running the job */
        FileUtils.deleteDirectory(new File(args[1]));
        /* set the hadoop system parameter */
        System.setProperty("hadoop.home.dir", "Replace this string with hadoop home directory location");
        if (args.length != 2) {
            System.err.println("Please specify the input and output path");
            System.exit(-1);
        }
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(MinMaxExample.class);
        job.setJobName("Find_Max_Min_Avg");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(EmployeeMinMaxCountMapper.class);
        job.setReducerClass(EmployeeMinMaxCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CustomMinMaxTuple.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class EmployeeMinMaxCountMapper extends Mapper<Object, Text, Text, CustomMinMaxTuple> {
    private CustomMinMaxTuple outTuple = new CustomMinMaxTuple();
    private Text departmentName = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        String[] field = data.split(",", -1);
        double salary = 0;
        if (null != field && field.length == 9 && field[7].length() >0) {
            salary=Double.parseDouble(field[7]);
            outTuple.setMin(salary);
            outTuple.setMax(salary);
            outTuple.setCount(1);
            departmentName.set(field[3]);
            context.write(departmentName, outTuple);
        }
    }
}

 class EmployeeMinMaxCountReducer extends Reducer<Text, CustomMinMaxTuple, Text, CustomMinMaxTuple> {
    private CustomMinMaxTuple result = new CustomMinMaxTuple();
    public void reduce(Text key, Iterable<CustomMinMaxTuple> values, Context context)
            throws IOException, InterruptedException {
        result.setMin(null);
        result.setMax(null);
        result.setCount(0);
        long sum = 0;
        for (CustomMinMaxTuple minMaxCountTuple : values) {
            if (result.getMax() == null || (minMaxCountTuple.getMax() > result.getMax())) {
                result.setMax(minMaxCountTuple.getMax());
            }
            if (result.getMin() == null || (minMaxCountTuple.getMin() < result.getMin())) {
                result.setMin(minMaxCountTuple.getMin());
            }
            sum = sum + minMaxCountTuple.getCount();
            result.setCount(sum);
        }
        context.write(new Text(key.toString()), result);
    }
}

class CustomMinMaxTuple implements Writable {
    private Double min = new Double(0);
    private Double max = new Double(0);
    private long count = 1;
    public Double getMin() {
        return min;
    }
    public void setMin(Double min) {
        this.min = min;
    }
    public Double getMax() {
        return max;
    }
    public void setMax(Double max) {
        this.max = max;
    }
    public long getCount() {
        return count;
    }
    public void setCount(long count) {
        this.count = count;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(min);
        dataOutput.writeDouble(max);
        dataOutput.writeLong(count);
        }


    @Override
    public void readFields(DataInput dataInput) throws IOException {
        min = dataInput.readDouble();
        max = dataInput.readDouble();
        count = dataInput.readLong();
    }

    public String toString() {
        return min + "\t" + max + "\t" + count;
    }

}