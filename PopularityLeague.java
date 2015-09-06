import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
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

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();
    FileSystem fs = FileSystem.get(conf);
    Path linkCountsTmpPath = new Path("/mp2/tmp/linkCounts");
    Path ranksTmpPath = new Path("/mp2/tmp/linkRanks");
    Path inputPaths = new Path(args[0]);
    Path resultPath = new Path(args[1]);

    // clean all directories
    fs.delete(linkCountsTmpPath, true);
    fs.delete(ranksTmpPath, true);
    fs.delete(resultPath, true);

    // Link Count Job Configuration
    Job linkJob = Job.getInstance(conf, "Popularity League");

    linkJob.setOutputKeyClass(IntWritable.class);
    linkJob.setOutputValueClass(IntWritable.class);

    linkJob.setMapperClass(LinkCountMap.class);
    linkJob.setReducerClass(LinkCountReduce.class);

    FileInputFormat.setInputPaths(linkJob, inputPaths);
    FileOutputFormat.setOutputPaths(linkJob, linkCountsTmpPath);

    linkJob.setJarByClass(PopularityLeague.class);
    linkJob.waitForCompletion(true);

    // Link Ranking Job Configuration
    Job rankJob = Job.getInstance(conf, "Popularity League");

    rankJob.setOutputKeyClass(IntWritable.class);
    rankJob.setOutputValueClass(IntWritable.class);

    rankJob.setMapOutputKeyClass(NullWritable.class);
    rankJob.setMapOutputValueClass(IntArrayWritable.class);

    rankJob.setMapperClass(TopLinksMap.class);
    rankJob.setReducerClass(TopLinksReduce.class);
    rankJob.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(rankJob, linkCountsTmpPath);
    FileOutputFormat.setOutputPath(rankJob, ranksTmpPath);

    rankJob.setInputFormatClass(KeyValueTextInputFormat.class);
    rankJob.setOutputFormatClass(KeyValueTextInputFormat.class);

    rankJob.setJarByClass(PopularityLeague.class);
    rankJob.waitForCompletion(true);

    // League Job Configuration
    Job leagueJob = Job.getInstance(conf, "Popularity League");

    leagueJob.setOutputKeyClass(IntWritable.class);
    leagueJob.setOutputValueClass(IntWritable.class);

    leagueJob.setMapOutputKeyClass(NullWritable.class);
    leagueJob.setMapOutputValueClass(IntArrayWritable.class);

    leagueJob.setMapperClass(LeagueRankMap.class);
    leagueJob.setReducerClass(LeagueRankReduce.class);
    leagueJob.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(leagueJob, ranksTmpPath);
    FileOutputFormat.setOutputPath(leagueJob, resultPath);

    leagueJob.SetInputFormatClass(KeyValueTextInputFormat.class);
    leagueJob.SetOutputFormatClass(TextOutputFormat.class);

    leagueJob.setJarByClass(PopularityLeague.class);
    return leagueJob.waitForCompletion(true) ? 0 : 1;
  }

  public static String readHDFSFile(String path, Configuration conf) throws IOException{
    Path pt = new Path(path);
    FileSystem fs = FileSystem.get(pt.toUri(), conf);
    FSDataInputStream file = fs.open(pt);
    BufferedReader buffIn = new BufferedReader(new InputStreamReader(file));

    StringBuilder everything = new StringBuilder();
    String line;
    while( (line = buffIn.readLine()) != null) {
      everything.append(line);
      everything.append("\n");
    }

    return everything.toString();
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
        if (linkIdStr.trim().isEmpty()) continue;
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
    private TreeSet<Pair<Integer, Integer>> rankMap = new TreeSet<Pair<Integer, Integer>>();

    @Override
    public void map(Text key, Text value, Context ctxt) throws IOException, InterruptedException {
      Integer count = Integer.parseInt(value.toString());
      Integer pageId = Integer.parseInt(key.toString());

      // treeset will sort on Key (pair[0]) so count *then* pageId
      rankMap.add(new Pair<Integer, Integer>(count, pageId));
    }

    @Override
    protected void cleanup(Context ctxt) throws IOException, InterruptedException {
      for (Pair<Integer, Integer> itm : rankMap) {
        Integer[] entry = { itm.first, itm.second };
        IntArrayWritable entryArr = new IntArrayWritable(entry);
        ctxt.write(NullWritable.get(), entryArr);
      }
    }
  }

  public static class TopLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
    private TreeSet<Pair<Integer, Integer>> rankMap = new TreeSet<Pair<Integer, Integer>>();

    @Override
    public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context ctxt) throws IOException, InterruptedException {
      for (IntArrayWritable val : values) {
        IntWritable[] rankEntry = (IntWritable[]) val.toArray();

        Integer linkBackCount = rankEntry[0].get();
        Integer pageId = rankEntry[1].get();

        // now that we have a shorter list we want to sort by pageId
        rankMap.add(new Pair<Integer, Integer>(linkBackCount, pageId));
      }

      for (Pair<Integer, Integer> rankEntry : rankMap.descendingSet()) {
        IntWritable count = new IntWritable(rankEntry.first);
        IntWritable pageId = new IntWritable(rankEntry.second);
        ctxt.write(pageId, count);
      }
    }
  }

  public static class LeagueRankMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
    List<Int> leagues;
    private TreeSet<Pair<Integer, Integer>> rankMap = new TreeSet<Pair<Integer, Integer>>();

    @Override
    protected void setup(Context ctxt) throws IOException, InterruptedException {
      // i. build leagues list
      Configuraiton conf = ctxt.getConfiguration();

      String leaguePath = conf.get("league");
      leagueStrings = readHDFSFile(leaguePath, conf).split("\n");

      for (String leagueId : leagueStrings) {
        if (leagueId.trim().isEmpty()) continue;
        leagues.add(Integer.parseInt(leagueId.trim()));
      }
    }

    @Override
    public void map(Text key, Text value, Context ctxt) throws IOException, InterrupedException {
      // ii. build page link-back ranks from previous reduce, key: pageId, value: linkback count
      //     filtering out everything except league values
      Integer pageId = Integer.parseInt(key.toString());
      Integer count = Integer.parseInt(value.toString());

      if (leagues.contains(pageId))
        rankMap.add(new Pair<Integer, Integer>(count, pageId));
    }

    @Override
    protected void cleanup(Context ctxt) throws IOException, InterruptedException {
      for (Pair<Integer, Integer> rank : rankMap) {
        Integer[] entry = { rank.first, rank.second };
        IntArrayWritable entryArr = new IntArrayWritable(entry);
        ctxt.write(NullWritable.get(), entryArr);
      }
    }
  }

  public static class LeagueRankReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
    private TreeSet<Pair<Integer, Integer>> rankMap = new TreeSet<Pair<Integer, Integer>>();

    @Override
    public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context ctxt) throws IOException, InterrupedException {
      // iv. get our full list for processing
      for (IntArrayWritable val : values) {
        IntWritable[] rankEntry = (IntWritable[]) val.toArray();

        Integer pageId = rankEntry[1].get();
        Integer linkBackCount = rankEntry[0].get();

        rankMap.add(new Pair<Integer, Integer>(linkBackCount, pageId));
      }

      // v. do the rank calculation & sort via rankMap where pair = {count, pageId}
      // note that rankMap.descendingSet() will return ranks from highest link count
      // to lowest link count
      for (Pair<Integer, Integer> entry : rankMap.descendingSet()) {
        // if we take the subset of a sorted set in desc. order from this
        // element to the bottom, the size of such set represents how many
        // elements we have that are lower than this one.
        // When we are at the bottom we will be getting the subset from
        // this to this so it will be zero
        Integer pageId = entry.second;
        Integer rank = rankMap.subset(entry, rankMap.last()).size();

        ctxt.write(pageId, rank);
      }

    }
  }
}

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
