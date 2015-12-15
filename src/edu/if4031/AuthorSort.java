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
import java.util.*;

public class AuthorSort {

    public static class Pair<K extends Comparable<K>,V> implements Comparable<Pair<K, V>> {
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
            return this.left.compareTo(kvPair.left);
        }
    }

    /**
     * CLASS DEFINITION
     * ----------------
     * Emits (publication_type, 1) for each end tag found.
     */
    public static class AuthorCountMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        private LongWritable longWritable1 = new LongWritable(1);
        private Text dummyText = new Text();

        private TreeMap<Long, Text> top = new TreeMap<>();
        private List<Pair<Long, Text>> pairList = new ArrayList<>();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String tmp[] = value.toString().split("\\t");
            Long num = Long.parseLong(tmp[1]);
            String name = tmp[0];
            dummyText.set(name);
            pairList.add(new Pair<>(num, dummyText));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(pairList);
            int num = 0;
            for (Map.Entry<Long, Text> entry : top.entrySet()) {
                ++num;
                if (num > 5) break;
                String tmp = entry.getKey().toString() + "\t" + entry.getValue().toString();
                dummyText.set(tmp);
                context.write(longWritable1, dummyText);
            }
        }
    }

    /**
     * CLASS DEFINITION
     * ----------------
     * Count the occurrence of a key.
     */
    public static class AuthorCountReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
        private Text author = new Text();
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<Pair<Long, Text>> top = new ArrayList<>();
            for (Text val : values) {
                String tmp[] = val.toString().split("\\t");
                System.out.println("testing " + tmp[0]);
                Long count = Long.valueOf(tmp[0]);
                author.set(tmp[1]);
                top.add(new Pair<>(count, author));
            }
            Collections.sort(top);
            for (Pair<Long, Text> p : top) {
                context.write(p.right, new LongWritable(p.left));
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
        job.setJarByClass(PublicationCount.class);

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
    public static final String[] PUBLICATION_END_TAGS = new String[]{
            "</author>"};
}
