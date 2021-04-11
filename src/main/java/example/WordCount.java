package example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.regex.Pattern;

public class WordCount {

    /* --- subsection 1.1 and 1.2 ------------------------------------------ */

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private Pattern punctuationPattern = Pattern.compile("\\p{P}");
        private Pattern wordPattern = Pattern.compile("[a-z]+");

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                String token = itr.nextToken().toLowerCase();             // convert words to lowercase
                token = punctuationPattern.matcher(token).replaceAll(""); // remove punctuation
                if (wordPattern.matcher(token).matches()) {               // remove non-latin-words
                    word.set(token);
                    context.write(word, one);
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

// <ExcludeStart>




    /* --- subsection 1.3 - [option 1] ------------------------------------- */

    public static class SingleKeyMapper
            extends Mapper<Text, Text, Text, Text> {

        private static final Text THE_KEY = new Text("key");
        public static final String SEPARATOR = ":";

        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String valuePair = key.toString() + SEPARATOR + value.toString();
            context.write(THE_KEY, new Text(valuePair));
        }
    }

    static class Pair<A, B> {
        public final A first;
        public final B second;
        public Pair(A first, B second) {
            this.first = first;
            this.second = second;
        }
    }

    public static class TopKReducer
            extends Reducer<Text, Text, Text, IntWritable> {

        public static final int K = 10;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        PriorityQueue<Pair<String, Integer>> pq = new PriorityQueue<>(
                        (p, q) -> p.second.compareTo(q.second));

                for (Text value : values) {
                    String[] parts = value.toString().split(SingleKeyMapper.SEPARATOR);
                    pq.add(new Pair<>(parts[0], Integer.parseInt(parts[1])));

                    if (pq.size() > K) { pq.poll(); }
            }

            ArrayList<Pair<String, Integer>> bestPairs = new ArrayList<>(pq);
            bestPairs.sort((p, q) -> q.second.compareTo(p.second));
            for (Pair<String, Integer> pair : bestPairs) {
                context.write(new Text(pair.first), new IntWritable(pair.second));
            }
        }
    }



    /* --- subsection 1.3 - [option 2] ------------------------------------- */

    public static class SwapMapper
            extends Mapper<Text, Text, IntWritable, Text> {

        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            int wordCount = Integer.parseInt(value.toString());
            context.write(new IntWritable(wordCount), key);
        }
    }

    public static class IdentityReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static class FilterTopKMapper
            extends Mapper<IntWritable, Text, Text, IntWritable> {
        int counter = 0;

        @Override
        public void map(IntWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (counter < 10) {
                context.write(value, key);
                counter++;
            }
        }
    }

    static class ReverseComparator extends WritableComparator {
        protected ReverseComparator() {
            super(IntWritable.class, true);
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            return -1 * w1.compareTo(w2);
        }
    }







    /* --- subsection 1.3 - [option 3] ------------------------------------- */

    public static class SortAndFilterTopKMapper
            extends Mapper<Text, Text, IntWritable, Text> {

        public static final int K = 10;
        private TreeSet<Pair<String, Integer>> set;

        @Override
        public void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);

            set = new TreeSet<>((p, q) -> {
                int keyComp = q.second.compareTo(p.second);
                if (keyComp == 0) { // sort by word name if frequency is equal --> != duplicate
                    return p.first.compareTo(q.first);
                }
                return keyComp;
            });
        }

        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            set.add(new Pair<String, Integer>(key.toString(), Integer.parseInt(value.toString())));

            if (set.size() > K) {
                set.pollLast();
            }
        }

        @Override
        public void cleanup(Context context)
                throws IOException, InterruptedException {

            for (Pair<String, Integer> pair : set) {
                context.write(new IntWritable(pair.second), new Text(pair.first));
            }

            super.cleanup(context);
        }
    }


    /* --- subsection 1.3 - [option 4] ------------------------------------- */

    // Don't use Hadoop. The number of words in the vocabulary should be much
    // much smaller than the potentially gigabytes or terabytes of text input
    // data. We can use normal programs, e.g.:
    //
    //     `sort -t $'\t' -k 2 -rn output/pass1/part-r-00000 | head -n 10`
    //

// <ExcludeEnd>




    /* --- main methods ---------------------------------------------------- */

    // subsection 1.1: counting word frequencies
    public static Job runWordCount(Path input, Path output) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(IntSumReducer.class); // subsection 1.2: combiner
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // FileInputFormat.setMaxInputSplitSize(job, 100000); // subsection 2.2

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

// <ExcludeStart>

    // subsection 1.3 - job chaining [option 1]
    public static Job runTopK_option1(Path input, Path output) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sort by frequency");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(SingleKeyMapper.class);
        job.setReducerClass(TopKReducer.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    // subsection 1.3 - job chaining [option 2]
    public static Job runTopK_option2(Path input, Path output) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sort by frequency");
        job.setJarByClass(WordCount.class);
        ChainMapper.addMapper(job, SwapMapper.class, Text.class, Text.class,
                IntWritable.class, Text.class, new Configuration(false));
        ChainReducer.setReducer(job, IdentityReducer.class, IntWritable.class,
                Text.class, IntWritable.class, Text.class, new Configuration(false));
        ChainReducer.addMapper(job, FilterTopKMapper.class,
                IntWritable.class, Text.class, Text.class, IntWritable.class,
                new Configuration(false));
        job.setNumReduceTasks(1);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setSortComparatorClass(ReverseComparator.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    // subsection 1.3 - job chaining [option 3]
    public static Job runTopK_option3(Path input, Path output) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sort by frequency");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(SortAndFilterTopKMapper.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setSortComparatorClass(ReverseComparator.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }
// <ExcludeEnd>

    public static void main(String[] args) throws Exception {
        Path input = new Path(args[0]);
        Path output1 = new Path(args[1], "pass1");

        Configuration conf = new Configuration();
        conf.set("spark.files.overwrite", "true");

        // subsection 1.1 - first map reduce job
        Job wordcountJob = runWordCount(input, output1);
        if (!wordcountJob.waitForCompletion(true)) {
            System.exit(1);
        }

// <ExcludeStart>

        // subsection 1.3 - job chaining [option 1]
        Job topkJob1 = runTopK_option1(output1, new Path(args[1], "pass2_option1"));
        if (!topkJob1.waitForCompletion(true)) {
            System.exit(2);
        }

        // subsection 1.3 - job chaining [option 2]
        Job topkJob2 = runTopK_option2(output1, new Path(args[1], "pass2_option2"));
        if (!topkJob2.waitForCompletion(true)) {
            System.exit(2);
        }

        // subsection 1.3 - job chaining [option 3]
        Job topkJob3 = runTopK_option3(output1, new Path(args[1], "pass2_option3"));
        if (!topkJob3.waitForCompletion(true)) {
            System.exit(2);
        }
// <ExcludeEnd>
    }
}