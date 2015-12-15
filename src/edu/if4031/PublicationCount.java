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

public class PublicationCount {

    /**
     * CLASS DEFINITION
     * ----------------
     * Emits (publication_type, 1) for each end tag found.
     */
    public static class PublicationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text publicationType = new Text();
        private final IntWritable publicationCount = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            for (String tag : PUBLICATION_END_TAGS) {
                if (line.contains(tag)) {
                    publicationType.set(tag.substring(2, tag.length() - 1));
                    context.write(publicationType, publicationCount);
                }
            }
        }
    }

    /**
     * CLASS DEFINITION
     * ----------------
     * Count the occurrence of a key.
     */
    public static class MySumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
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

        job.setMapperClass(PublicationMapper.class);
        job.setCombinerClass(MySumReducer.class);
        job.setReducerClass(MySumReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static final String JOB_DESCRIPTION = "dblp publication count";
    public static final String USAGE = "Usage: publicationcount <in> <out>";
    public static final String[] PUBLICATION_END_TAGS = new String[]{
            "</article>", "</inproceedings>", "</phdthesis>", "</masterthesis>"};
}
