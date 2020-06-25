package ca.uwaterloo.cs451.a3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

public class BooleanRetrievalCompressed extends Configured implements Tool {
  private MapFile.Reader[] indexes;
  private FSDataInputStream collection;
  private Stack<Set<Integer>> stack;
  private int numReducers;

  private BooleanRetrievalCompressed() {}

  private void initialize(String indexPath, String collectionPath, FileSystem fs) throws IOException {
    // set reader for all input files as we have multiple reducers now
    FileStatus[] status = fs.listStatus(new Path(indexPath));
    int numFiles = status.length;
    numReducers = numFiles - 1;
    indexes = new MapFile.Reader[numReducers];

    int count = 0, i = 0;
    while (count < numFiles) {
      String curFilePath = status[count].getPath().toString();
      if (!curFilePath.contains("SUCCESS")) {
        indexes[i] = new MapFile.Reader(new Path(curFilePath), fs.getConf());
        i++;
      }
      count++;
    }

    collection = fs.open(new Path(collectionPath));
    stack = new Stack<>();
  }

  private void runQuery(String q) throws IOException {
    String[] terms = q.split("\\s+");

    for (String t : terms) {
      if (t.equals("AND")) {
        performAND();
      } else if (t.equals("OR")) {
        performOR();
      } else {
        pushTerm(t);
      }
    }

    Set<Integer> set = stack.pop();

    for (Integer i : set) {
      String line = fetchLine(i);
      System.out.println(i + "\t" + line);
    }
  }

  private void pushTerm(String term) throws IOException {
    stack.push(fetchDocumentSet(term));
  }

  private void performAND() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<>();

    for (int n : s1) {
      if (s2.contains(n)) {
        sn.add(n);
      }
    }

    stack.push(sn);
  }

  private void performOR() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<>();

    for (int n : s1) {
      sn.add(n);
    }

    for (int n : s2) {
      sn.add(n);
    }

    stack.push(sn);
  }

  private Set<Integer> fetchDocumentSet(String term) throws IOException {
    Set<Integer> set = new TreeSet<>();

    for (PairOfInts pair : fetchPostings(term)) {
      set.add(pair.getLeftElement());
    }

    return set;
  }

  private ArrayListWritable<PairOfInts> fetchPostings(String term) throws IOException {
    Text key = new Text(); // key(term) read
    BytesWritable value = new BytesWritable(); // byte value read associated with key
    ArrayListWritable<PairOfInts> postingList = new ArrayListWritable<PairOfInts>(); // return value

    // get the partition number(i.e. the file number) for the given term
    int fileNum = (term.hashCode() & Integer.MAX_VALUE) % numReducers;
    key.set(term);
    indexes[fileNum].get(key, value);

    // parse the Byte values associated with the key
    byte[] dataRead = value.getBytes();
    ByteArrayInputStream byteArrayInput = new ByteArrayInputStream(dataRead);
    DataInputStream dataInput = new DataInputStream(byteArrayInput);

    int df = WritableUtils.readVInt(dataInput);
    int docID = 0;
    int count = 0;
    for (int i = 0; i < df; i++) {
      docID += WritableUtils.readVInt(dataInput);
      count = WritableUtils.readVInt(dataInput);
      postingList.add(new PairOfInts(docID, count));
    }

    // close input stream
    byteArrayInput.reset();
    dataInput.close();

    return postingList;
  }

  public String fetchLine(long offset) throws IOException {
    collection.seek(offset);
    BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

    String d = reader.readLine();
    return d.length() > 80 ? d.substring(0, 80) + "..." : d;
  }

  private static final class Args {
    @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
    String index;

    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
    String collection;

    @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
    String query;
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

    if (args.collection.endsWith(".gz")) {
      System.out.println("gzipped collection is not seekable: use compressed version!");
      return -1;
    }

    FileSystem fs = FileSystem.get(new Configuration());

    initialize(args.index, args.collection, fs);

    System.out.println("Query: " + args.query);
    long startTime = System.currentTimeMillis();
    runQuery(args.query);
    System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

    return 1;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BooleanRetrievalCompressed(), args);
  }
}
