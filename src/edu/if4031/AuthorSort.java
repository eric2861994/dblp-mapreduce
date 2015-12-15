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
import java.util.TreeMap;

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

        private final IntWritable dummyKey = new IntWritable();
        private final Text numOfPublications_author = new Text();

        private final TreeMap<Pair<Integer, String>, Boolean> top5 = new TreeMap<>();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String tmp[] = value.toString().split("\\t");
            Integer num = Integer.parseInt(tmp[1]);
            String name = tmp[0];

            top5.put(new Pair<>(num, name), Boolean.TRUE);
            if (top5.size() > 5) {
                top5.remove(top5.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            while (!top5.isEmpty()) {
                Pair<Integer, String> last = top5.lastKey();
                top5.remove(last);

                String numOfPublications_authorString = last.left.toString() + "\t" + last.right;
                numOfPublications_author.set(numOfPublications_authorString);
                context.write(dummyKey, numOfPublications_author);
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

            TreeMap<Pair<Integer, String>, Boolean> top5 = new TreeMap<>();
            for (Text val : values) {
                String tmp[] = val.toString().split("\\t");
                Integer count = Integer.valueOf(tmp[0]);

                top5.put(new Pair<>(count, tmp[1]), Boolean.TRUE);
                if (top5.size() > 5) {
                    top5.remove(top5.firstKey());
                }
            }

            while (!top5.isEmpty()) {
                Pair<Integer, String> last = top5.lastKey();
                top5.remove(last);

                numberOfPublications.set(last.left);
                author.set(last.right);
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
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(AuthorCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static final String JOB_DESCRIPTION = "dblp author sort";
    public static final String USAGE = "Usage: authorsort <in> <out>";
}
