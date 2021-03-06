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

public class AuthorCount {

    /**
     * CLASS DEFINITION
     * ----------------
     * Emits (author_name, 1) for each author found.
     */
    public static class AuthorCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text authorName = new Text();
        private final IntWritable one = new IntWritable(1);


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            if (line.contains("<author>")) {
                int idx = line.indexOf("<author>");
                int idxlast = line.indexOf("</author>");

                // authorname start at the end of <author> and ends before the start of </author>
                line = line.substring(idx + 8, idxlast);

                authorName.set(line);
                context.write(authorName, one);
            }
        }
    }

    /**
     * CLASS DEFINITION
     * ----------------
     */
    public static class AuthorCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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
        job.setJar("authorcount.jar");
        job.setJarByClass(AuthorCount.class);

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
    public static final String USAGE = "Usage: authorcount <in> <out>";
}
