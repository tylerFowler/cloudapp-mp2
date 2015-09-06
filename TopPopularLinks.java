import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.lang.Integer;
import java.util.StringTokenizer;
import java.util.TreeSet;

// >>> Don't Change
public class TopPopularLinks extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(TopPopularLinks.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopPopularLinks(), args);
        System.exit(res);
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }
// <<< Don't Change

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();
    FileSystem fs = FileSystem.get(conf);
    Path tmpPath = new Path("/mp2/tmp");
    fs.delete(tmpPath, true);

    // Link Count Job Configuration
    Job linkJob = Job.getInstance(conf, "Top Popular Links");

    linkJob.setOutputKeyClass(IntWritable.class);
    linkJob.setOutputValueClass(IntWritable.class);

    linkJob.setMapperClass(LinkCountMap.class);
    linkJob.setReducerClass(LinkCountReduce.class);

    FileInputFormat.setInputPaths(linkJob, new Path(args[0]));
    FileOutputFormat.setOutputPath(linkJob, tmpPath);

    linkJob.setJarByClass(TopPopularLinks.class);
    linkJob.waitForCompletion(true);

    // Top Link Job Configuration
    Job topLinksJob = Job.getInstance(conf, "Top Popular Links");

    topLinksJob.setOutputKeyClass(IntWritable.class);
    topLinksJob.setOutputValueClass(IntWritable.class);

    topLinksJob.setMapOutputKeyClass(NullWritable.class);
    topLinksJob.setMapOutputValueClass(IntArrayWritable.class);

    topLinksJob.setMapperClass(TopLinksMap.class);
    topLinksJob.setReducerClass(TopLinksReduce.class);
    topLinksJob.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(topLinksJob, tmpPath);
    FileOutputFormat.setOutputPath(topLinksJob, new Path(args[1]));

    topLinksJob.setInputFormatClass(KeyValueTextInputFormat.class);
    topLinksJob.setOutputFormatClass(TextOutputFormat.class);

    topLinksJob.setJarByClass(TopPopularLinks.class);
    return topLinksJob.waitForCompletion(true) ? 0 : 1;
  }

  public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
    @Override
    public void map(Object key, Text line, Context ctxt) throws IOException, InterruptedException {
      String[] input = line.toString().split(":");

      Integer pageId = Integer.parseInt(input[0].replace(':', ' ').trim());
      String[] pageLinks = input[1].split(" ");

      // Flip the format upside down so that each *linked to*
      // page has a corresponding page that *links* to it.

      // This method completely excludes orphaned links but since
      // this is a popularity contest we don't care, and even better,
      // it makes the reducer dataset smaller
      for (String linkIdStr : pageLinks) {
        if (linkIdStr.isEmpty()) continue;
        Integer linkId = Integer.parseInt(linkIdStr.trim());
        ctxt.write(new IntWritable(linkId), new IntWritable(1));
      }
    }
  }

  public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    // we simply need to aggregate the number of counts
    // this is more of a map than a reduce.. the mapper already reduced
    // by excluding orphaned pages

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context ctxt) throws IOException, InterruptedException {
      Integer linkBackCount = 0;
      for (IntWritable linkId : values) {
        linkBackCount += linkId.get();
      }

      ctxt.write(key, new IntWritable(linkBackCount));
    }
  }

  public static class TopLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
    Integer N;
    private TreeSet<Pair<Integer, Integer>> rankMap = new TreeSet<Pair<Integer, Integer>>();

    @Override
    protected void setup(Context context) throws IOException,InterruptedException {
        Configuration conf = context.getConfiguration();
        this.N = conf.getInt("N", 10);
    }

    @Override
    public void map(Text key, Text value, Context ctxt) throws IOException, InterruptedException {
      Integer pageId = Integer.parseInt(key.toString());
      Integer count = Integer.parseInt(value.toString());

      rankMap.add(new Pair<Integer, Integer>(pageId, count));

      if (rankMap.size() > this.N) {
        rankMap.remove(rankMap.first());
      }
    }

    @Override
    protected void cleanup(Context ctxt) throws IOException, InterruptedException {
      for (Pair<Integer, Integer> itm : rankMap) {
        Integer [] entry = { itm.first, itm.second };
        IntArrayWritable entryArr = new IntArrayWritable(entry);
        ctxt.write(NullWritable.get(), entryArr);
      }
    }
  }

  public static class TopLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
    Integer N;
    private TreeSet<Pair<Integer, Integer>> rankMap = new TreeSet<Pair<Integer, Integer>>();

    @Override
    protected void setup(Context context) throws IOException,InterruptedException {
      Configuration conf = context.getConfiguration();
      this.N = conf.getInt("N", 10);
    }

    @Override
    public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context ctxt) throws IOException, InterruptedException {
      for (IntArrayWritable val : values) {
        IntWritable[] rankEntry = (IntWritable[]) val.toArray();

        Integer pageId = rankEntry[0].get();
        Integer linkBackCount = rankEntry[1].get();

        rankMap.add(new Pair<Integer, Integer>(pageId, linkBackCount));

        if (rankMap.size() > this.N) {
          rankMap.remove(rankMap.first());
        }
      }

      for (Pair<Integer, Integer> rankEntry : rankMap.descendingSet()) {
        IntWritable pageId = new IntWritable(rankEntry.first);
        IntWritable count = new IntWritable(rankEntry.second);
        ctxt.write(pageId, count);
      }
    }
  }
}

// >>> Don't Change
class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

  public final A first;
  public final B second;

  public Pair(A first, B second) {
      this.first = first;
      this.second = second;
  }

  public static <A extends Comparable<? super A>,
          B extends Comparable<? super B>>
  Pair<A, B> of(A first, B second) {
      return new Pair<A, B>(first, second);
  }

  @Override
  public int compareTo(Pair<A, B> o) {
      int cmp = o == null ? 1 : (this.first).compareTo(o.first);
      return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
  }

  @Override
  public int hashCode() {
      return 31 * hashcode(first) + hashcode(second);
  }

  private static int hashcode(Object o) {
      return o == null ? 0 : o.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
      if (!(obj instanceof Pair))
          return false;
      if (this == obj)
          return true;
      return equal(first, ((Pair<?, ?>) obj).first)
              && equal(second, ((Pair<?, ?>) obj).second);
  }

  private boolean equal(Object o1, Object o2) {
      return o1 == o2 || (o1 != null && o1.equals(o2));
  }

  @Override
  public String toString() {
      return "(" + first + ", " + second + ')';
  }
}
// <<< Don't Change
