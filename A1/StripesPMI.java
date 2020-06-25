package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.map.HMapStIW;
import tl.lin.data.map.HashMapWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * StripesPMI
 */
public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);
  private static final int MAX_WORD = 40;

  // Mapper: adds one to line counter for every call and (word, 1) if the word appears.
  public static final class MyLineWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();
    public enum LineNumCounter { LINENUM };
    
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      int numWord = 0;
      Map<String, Integer> word_map = new HashMap<String, Integer>();
      for (String word : Tokenizer.tokenize(value.toString())) {
        if (numWord < MAX_WORD) {
          if (!word_map.containsKey(word)) {
            word_map.put(word, 1);
          }
          numWord++;
        } else {
          break;
        }
      }
      
      for (Map.Entry<String, Integer> mapElement : word_map.entrySet()) {
        String key_value = mapElement.getKey();
        WORD.set(key_value);
        context.write(WORD, ONE);
      }

      Counter counter = context.getCounter(LineNumCounter.LINENUM);
      counter.increment(1L);
    }
  }

  // Reducer: sums up all the counts.
  public static final class MyLineWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final IntWritable SUM = new IntWritable();
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get(); 
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  // Mapper: for each word x, add (y, 1) to it's pair list where y not equal to x.
  public static final class MyPairMapper extends Mapper<LongWritable, Text, Text, HMapStIW> {
    private static final Text KEYWORD = new Text();
    private static final HMapStIW PAIRLIST = new HMapStIW();
    
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      int numWord = 0;
      List<String> words = Tokenizer.tokenize(value.toString());

      if (words.size() < 2) return;
      Set<String> wordsSet = new HashSet<String>();
      for (int i = 0; i < Math.min(words.size(), 40); i++) {
        wordsSet.add(words.get(i));
      }

      String[] uniqueWords = new String[wordsSet.size()];
      uniqueWords = wordsSet.toArray(uniqueWords);
      for (String word : uniqueWords) {
        PAIRLIST.clear();
        KEYWORD.set(word);
        for (int j = 0; j < uniqueWords.length; j++) {
          if (!uniqueWords[j].equals(word)) {
            PAIRLIST.put(uniqueWords[j], 1);
          }
        }
        context.write(KEYWORD, PAIRLIST);
      }
    }
  }

  public static final class MyPairCombiner extends Reducer<Text, HMapStIW, Text, HMapStIW> {
    
    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW pairs = new HMapStIW();
      while (iter.hasNext()) {
        pairs.plus(iter.next());
      }
      context.write(key, pairs);
    }
  }

  // Reducer: sums up all the counts.
  public static final class MyPairReducer extends Reducer<Text, HMapStIW, Text, HashMapWritable> {
    // Reuse objects.
    private static final Text KEY = new Text();
    private static final HashMapWritable RESULTLIST = new HashMapWritable();
    private static final Map<String, Float> word_map = new HashMap<String, Float>();
    private static long lineNum;
    private static int threshold;

    @Override
    // set up necessary variable to calculate PMI
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      lineNum = conf.getLong("lineCount", 0L);
      threshold = conf.getInt("threshold", 0);

      FileSystem fs = FileSystem.get(conf);
      FileStatus[] fileList = fs.globStatus(new Path("tmp/part-r-*"));
      for (FileStatus file : fileList) {
        FSDataInputStream fsdis = fs.open(file.getPath());
        BufferedReader br = new BufferedReader(new InputStreamReader(fsdis, "UTF-8"));
        String str = br.readLine();
        while (str != null) {
          String[] value = str.split("\\s+");
          if (value.length == 2) {
            word_map.put(value[0], Float.parseFloat(value[1]));
          }
          str = br.readLine();
        }
        br.close();
      }
    }

    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
    
      // Sum up values.
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW pairs = new HMapStIW();    
      while (iter.hasNext()) {
        pairs.plus(iter.next());
      }

      String mainKey = key.toString();
      KEY.set(mainKey);
      RESULTLIST.clear();
      for(String mapKey : pairs.keySet()) {
        int sum = pairs.get(mapKey);
        if (sum >= threshold) {
          float pmi = (float) Math.log10((double)sum * lineNum / (double)(word_map.get(mainKey) * word_map.get(mapKey)));
          PairOfFloatInt PMI = new PairOfFloatInt();
          PMI.set(pmi, sum);
          RESULTLIST.put(mapKey, PMI);
        }
      }
      if (!RESULTLIST.isEmpty()) {
        context.write(KEY, RESULTLIST);
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private StripesPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-threshold", metaVar = "[num]", usage = "threshold of co-occurrence")
    int threshold = 1;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);
    LOG.info(" - threshold of co-occurrence: " + args.threshold);

    // First MapReduce Job
    String preservedDataPath = "tmp/";
    Configuration conf = getConf();
    conf.set("preservedDataPath", preservedDataPath);
    conf.set("threshold", Integer.toString(args.threshold));
    Job job1 = Job.getInstance(conf);
    job1.setJobName(StripesPMI.class.getSimpleName());
    job1.setJarByClass(StripesPMI.class);

    job1.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job1, new Path(args.input));
    FileOutputFormat.setOutputPath(job1, new Path(preservedDataPath));

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    job1.setOutputFormatClass(TextOutputFormat.class);

    job1.setMapperClass(MyLineWordMapper.class);
    job1.setCombinerClass(MyLineWordReducer.class);
    job1.setReducerClass(MyLineWordReducer.class);

    job1.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job1.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job1.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");
    // Delete the output directory if it exists already.
    Path outputDir = new Path(preservedDataPath);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");


    // Second MapReduce Job
    long lineCount = job1.getCounters().findCounter(MyLineWordMapper.LineNumCounter.LINENUM).getValue();
    conf.setLong("lineCount", lineCount);
    Job job2 = Job.getInstance(conf);
    job2.setJobName(StripesPMI.class.getSimpleName());
    job2.setJarByClass(StripesPMI.class);

    job2.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job2, new Path(args.input));
    FileOutputFormat.setOutputPath(job2, new Path(args.output));

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(HMapStIW.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(HashMapWritable.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    job2.setMapperClass(MyPairMapper.class);
    job2.setCombinerClass(MyPairCombiner.class);
    job2.setReducerClass(MyPairReducer.class);

    job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");
    // Delete the output directory if it exists already.
    outputDir = new Path(args.output);
    FileSystem.get(conf).delete(outputDir, true);
    
    startTime = System.currentTimeMillis();
    job2.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}