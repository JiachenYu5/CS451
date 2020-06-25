package ca.uwaterloo.cs451.a4;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.omg.CORBA.FloatHolder;
import tl.lin.data.array.ArrayListOfIntsWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectFloat;
import tl.lin.data.queue.TopScoredObjects;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Iterator;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

  private static final String SOURCELIST = "source.list";
  private static final String TOPNUM = "topnum";

  private static class MapClass extends Mapper<IntWritable, PageRankNode, PairOfInts, FloatWritable> {
    private static TopScoredObjects[] queueForNodes;
    private static final PairOfInts NODEPAIR = new PairOfInts();
    private static final FloatWritable SCORE = new FloatWritable();
    private static int sourcesNum;
    private static int topNum;

    @Override
    public void setup(Context context) throws IOException {
      String[] sources = context.getConfiguration().getStrings(SOURCELIST, "");
      sourcesNum = sources.length;
      topNum = context.getConfiguration().getInt(TOPNUM, 1);
      queueForNodes = new TopScoredObjects[sourcesNum];
      for (int i = 0; i < sourcesNum; i++) {
        queueForNodes[i] = new TopScoredObjects<Integer>(topNum);
      }
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException, InterruptedException {
      for (int i = 0; i < sourcesNum; i++) {
        queueForNodes[i].add(node.getNodeId(), node.getPageRank().get(i));
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      for (int i = 0; i < sourcesNum; i++) {
        for (PairOfObjectFloat<Integer> pair : queueForNodes[i].extractAll()) {
          NODEPAIR.set(pair.getLeftElement(), i);
          SCORE.set(pair.getRightElement());
          context.write(NODEPAIR, SCORE);
        }
      }
    }
  }

  private static class ReduceClass extends Reducer<PairOfInts, FloatWritable, FloatWritable, IntWritable> {
    private static TopScoredObjects[] queueForNodes;
    private static int[] sourceList;
    private static final IntWritable NODE = new IntWritable();
    private static final FloatWritable SCORE = new FloatWritable();
    private static int sourcesNum;
    private static int topNum;

    @Override
    public void setup(Context context) throws IOException {
      String[] sources = context.getConfiguration().getStrings(SOURCELIST, "");
      sourcesNum = sources.length;
      topNum = context.getConfiguration().getInt(TOPNUM, 1);
      sourceList = new int[sourcesNum];
      queueForNodes = new TopScoredObjects[sourcesNum];
      for (int i = 0; i < sourcesNum; i++) {
        sourceList[i] = Integer.parseInt(sources[i]);
        queueForNodes[i] = new TopScoredObjects<Integer>(topNum);
      }
    }

    @Override
    public void reduce(PairOfInts nids, Iterable<FloatWritable> iterable, Context context) throws IOException, InterruptedException {
      Iterator<FloatWritable> iter = iterable.iterator();
      queueForNodes[nids.getRightElement()].add(nids.getLeftElement(), iter.next().get());
      // Iterable should contain only one value;
      if (iter.hasNext()) throw new RuntimeException();
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      for (int i = 0; i < sourcesNum; i++) {
        for (PairOfObjectFloat<Integer> pair : queueForNodes[i].extractAll()) {
          NODE.set(pair.getLeftElement());
          SCORE.set((float)StrictMath.exp(pair.getRightElement()));
          context.write(SCORE, NODE);
        }
      }
    }
  }

  public ExtractTopPersonalizedPageRankNodes() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String TOP = "top";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("top num nodes of highest score").create(TOP));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("list of sources").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP) || !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int top = Integer.parseInt(cmdline.getOptionValue(TOP));
    String sourceList = cmdline.getOptionValue(SOURCES);

    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - inputDir: " + inputPath);
    LOG.info(" - outputDir: " + outputPath);
    LOG.info(" - top: " + top);
    LOG.info(" - sources: " + sourceList);

    Configuration conf = getConf();
    conf.setInt(TOPNUM, top);
    conf.setStrings(SOURCELIST, sourceList);
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);

    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getSimpleName() + ":" + inputPath);
    job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(PairOfInts.class);
    job.setMapOutputValueClass(FloatWritable.class);

    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(MapClass.class);
    job.setReducerClass(ReduceClass.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    printResult(outputPath, conf, top, sourceList);

    return 0;
  }

  // help function for printing output
  private void printResult(String outputPath, Configuration conf, int top, String sourceList) throws Exception {
    String[] sources = sourceList.split(",");
    Path path = new Path(outputPath + "/part-r-00000");
    FileSystem fs = FileSystem.get(conf);
    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
    int node, count = 0;
    float score;
    String str;
    while ((str = br.readLine()) != null) {
      if (count % top == 0) {
        System.out.println();
        System.out.println("Source: " + sources[count / top]);
      }
      String[] pairs = str.split("\\t");
      score = Float.parseFloat(pairs[0]);
      node = Integer.parseInt(pairs[1]);
      System.out.println(String.format("%.5f %d", score, node));
      count++;
    }
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
  }
}