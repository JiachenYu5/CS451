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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * PairsPMI
 */
public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);
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

  // Mapper: add (pair, 1) if the co-occurrence pair occurs in a line.
  public static final class MyPairMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private static final PairOfStrings COPAIRS = new PairOfStrings();
    
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      int numWord = 0;
      List<String> words = Tokenizer.tokenize(value.toString());

      if (words.size() < 2) return;
      Map<String, Integer> pair_map = new HashMap<String, Integer>();
      for (int i = 0; i < words.size() - 1; i++) {
	if ( i < MAX_WORD ) {
	  for (int j = i + 1; j < words.size(); j++) {
	    if (j <= MAX_WORD) {
	      if (!words.get(i).equals(words.get(j))) {
	        String word_pair1 = words.get(i) + " " + words.get(j);
	        String word_pair2 = words.get(j) + " " + words.get(i);
	        if (!pair_map.containsKey(word_pair1)) {
	          pair_map.put(word_pair1, 1);
	          pair_map.put(word_pair2, 1);
	        }
	      }
	    } else {
	      break;
	    }
	  }
	} else {
	  break;
	}
      }

      for (Map.Entry<String, Integer> mapElement : pair_map.entrySet()) {
	String key_value = mapElement.getKey();
	String[] pairs= key_value.split("\\s");
	COPAIRS.set(pairs[0], pairs[1]);
	context.write(COPAIRS,ONE);
      }
    }
  }

  
  public static final class MyPairCombiner extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
    private static final IntWritable SUM = new IntWritable();
    
    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  // Reducer: sums up all the counts.
  public static final class MyPairReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, PairOfFloatInt> {
    // Reuse objects.
    private static final PairOfFloatInt PMI = new PairOfFloatInt();
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
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
    
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      if (sum >= threshold) {
	String x = key.getLeftElement();
        String y = key.getRightElement();
	float pmi = (float) Math.log10((double)sum * lineNum / (double)(word_map.get(x) * word_map.get(y)));
	PMI.set(pmi, sum);
	context.write(key, PMI);
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private PairsPMI() {}

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

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
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
    job1.setJobName(PairsPMI.class.getSimpleName());
    job1.setJarByClass(PairsPMI.class);

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
    job2.setJobName(PairsPMI.class.getSimpleName());
    job2.setJarByClass(PairsPMI.class);

    job2.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job2, new Path(args.input));
    FileOutputFormat.setOutputPath(job2, new Path(args.output));

    job2.setMapOutputKeyClass(PairOfStrings.class);
    job2.setMapOutputValueClass(IntWritable.class);
    job2.setOutputKeyClass(PairOfStrings.class);
    job2.setOutputValueClass(PairOfFloatInt.class);
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
    ToolRunner.run(new PairsPMI(), args);
  }
}
