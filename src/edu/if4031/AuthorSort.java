package edu.if4031;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class AuthorSort {

    /**
     * CLASS DEFINITION
     * ----------------
     * Emits (publication_type, 1) for each end tag found.
     */
    public static class AuthorCountMapper extends Mapper<Text, LongWritable, LongWritable, Text> {

        private final Text publicationType = new Text();
        private final IntWritable publicationCount = new IntWritable(1);
        private TreeMap<Long, Text> top = new TreeMap<>();

        @Override
        protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
            top.put(value.get(), key);
            if (top.size() > 5) {
                top.remove(top.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Long, Text> entry : top.entrySet()) {
                String tmp = entry.getKey() + '\t' + entry.getValue().toString();
                context.write(new LongWritable(1), new Text(tmp));
            }
        }
    }

    /**
     * CLASS DEFINITION
     * ----------------
     * Count the occurrence of a key.
     */
    public static class AuthorCountReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

        private final IntWritable result = new IntWritable();

        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            TreeMap<Long, Text> top = new TreeMap<>();
            for (Text val : values) {
                String tmp[] = val.toString().split("\\t");
                Long count = new Long(tmp[0]);
                Text author = new Text(tmp[1]);
                top.put(count, author);
                if (top.size() > 5) {
                    top.remove(top.firstKey());
                }
            }
            for (Map.Entry<Long, Text> entry : top.entrySet()) {
                context.write(entry.getValue(), new LongWritable(entry.getKey().longValue()));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // invalid arguments
        if (otherArgs.length != 2) {
            System.err.println(USAGE);
            System.exit(2);
        }

        Job job = Job.getInstance(conf, JOB_DESCRIPTION);
        job.setJar("publicationcount.jar");
        job.setJarByClass(PublicationCount.class);

        job.setMapperClass(AuthorCountMapper.class);
        job.setCombinerClass(AuthorCountReducer.class);
        job.setReducerClass(AuthorCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    public static final String JOB_DESCRIPTION = "dblp author count";
    public static final String USAGE = "Usage: publicationcount <in> <out>";
    public static final String[] PUBLICATION_END_TAGS = new String[]{
            "</author>"};
}
