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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AuthorSort {

    public static class Pair<K extends Comparable<K>, V> implements Comparable<Pair<K, V>> {
        public Pair() {
        }

        public Pair(K left, V right) {
            this.left = left;
            this.right = right;
        }

        public K left;
        public V right;

        @Override
        public int compareTo(Pair<K, V> kvPair) {
            return left.compareTo(kvPair.left);
        }
    }

    /**
     * CLASS DEFINITION
     * ----------------
     * Emits top 5 authors based on number of publications.
     */
    public static class AuthorCountMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private final IntWritable numberOfPublications = new IntWritable();
        private final Text author = new Text();

        private List<Pair<Integer, String>> pairList = new ArrayList<>();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String tmp[] = value.toString().split("\\t");
            Integer num = Integer.parseInt(tmp[1]);
            String name = tmp[0];

            pairList.add(new Pair<>(num, name));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(pairList);

            int lowerBound = Math.max(0, pairList.size() - 5);
            for (int idx = pairList.size() - 1; idx >= lowerBound; idx--) {
                Pair<Integer, String> pair = pairList.get(idx);

                numberOfPublications.set(pair.left);
                author.set(pair.right);
                context.write(numberOfPublications, author);
            }
        }
    }

    /**
     * CLASS DEFINITION
     * ----------------
     * Combine and output top 5 authors based on number of publications.
     */
    public static class AuthorCountReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

        private final Text author = new Text();
        private final IntWritable numberOfPublications = new IntWritable();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<Pair<Integer, String>> top = new ArrayList<>();
            for (Text val : values) {
                String tmp[] = val.toString().split("\\t");
                Integer count = Integer.valueOf(tmp[0]);

                top.add(new Pair<>(count, tmp[1]));
            }

            Collections.sort(top);
            int lowerBound = Math.min(0, top.size() - 5);
            for (int idx = top.size() - 1; idx >= lowerBound; idx--) {
                Pair<Integer, String> p = top.get(idx);

                author.set(p.right);
                numberOfPublications.set(p.left);
                context.write(author, numberOfPublications);
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
        job.setJar("authorsort.jar");
        job.setJarByClass(AuthorSort.class);

        job.setMapperClass(AuthorCountMapper.class);
        job.setReducerClass(AuthorCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static final String JOB_DESCRIPTION = "dblp author sort";
    public static final String USAGE = "Usage: authorsort <in> <out>";
}
