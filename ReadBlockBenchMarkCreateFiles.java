package org.apache.hadoop.raid.tools;
/*
 * Creates one file for each block size and read type combination
 */
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.WriteOptions;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReadBlockBenchMarkCreateFiles extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(ReadBlockBenchMarkCreateFiles.class);
  private static String TEST_ROOT_DIR = System.getProperty("test.build.data","/benchmarks/ReadBlockBenchMark");
  private static Path CONTROL_DIR = new Path(TEST_ROOT_DIR, "io_control");
  private static Path OUTPUT_DIR = new Path(TEST_ROOT_DIR, "io_output");
  private static Path DATA_DIR = new Path(TEST_ROOT_DIR, "io_data");
  private static final String BASE_FILE_NAME = "test_read_block_";

  public static int numFiles = 25;
  public static final long[] blockSizes = new long[] {64*1024, 4*1024*1024, 64*1024*1024,256*1024*1024};
  public static final String[] readTypes = new String[] {"f10", "PB1", "PB2"};//, "f11","f12","f13"};
  public static int numblocks = 13; // make it 20 to test f11, f12 and f13

  {
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("core-site.xml");

    Configuration.addDefaultResource("raid-default.xml");
    Configuration.addDefaultResource("raid-site.xml");
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.fairscheduler.pool", "di.sla");
    conf.setBoolean("mapreduce.job.user.classpath.first", true);
    FileSystem fs = FileSystem.get(conf);

    createControlFiles(fs, numFiles, conf);

    fs.delete(OUTPUT_DIR, true);
    runBenchmark(OUTPUT_DIR, conf);
    return 0;
  }


  private static void createControlFiles(FileSystem fs, int numFiles, 
      Configuration conf) throws IOException {
    LOG.info("creating control file: " + numFiles + " files");

    fs.delete(CONTROL_DIR, true);
    for (int i = 0; i < numFiles; i++) {
      String name = getFileName(i);
      Path controlFile = new Path(CONTROL_DIR, "in_file_" + name);
      SequenceFile.Writer writer = null;
      try {
        writer = 
            SequenceFile.createWriter(fs, conf, controlFile,
                Text.class, LongWritable.class,
                CompressionType.NONE);
        writer.append(new Text(name), new LongWritable(1));
      } finally {
        if (writer != null) {
          writer.close();
        }
        writer = null;
      }
    }
    LOG.info("created control files.");
  }

  private static String getFileName(int index) {
    return BASE_FILE_NAME + Integer.toString(index) ;
  }

  private static String getFileNameWithBlockSize(String base, long blockSize) {
    return base + '_' + Long.toString(blockSize);
  }

  public static String getFileNameWithBlockSizeAndReadType(String base, long blockSize, String readType) {
    return base + '_' + blockSize + '_' + readType;
  }

  private static void runBenchmark(Path outputDir, Configuration conf) 
      throws IOException{
    JobConf job = new JobConf(conf, ReadBlockBenchMarkCreateFiles.class);

    FileInputFormat.setInputPaths(job, CONTROL_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(CreateFileMapper.class);

    FileOutputFormat.setOutputPath(job, outputDir);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    JobClient.runJob(job);
  }

  private static void createFile(FileSystem fs, Path filePath, 
      long blockSize, int bufferSize, int numBlocks, Reporter reporter) throws IOException {
    Random rand = new Random();
    DistributedFileSystem dfs = DFSUtil.convertToDFS(fs);
    WriteOptions options = new WriteOptions();
    options.setFadvise(4);
    OutputStream out = dfs.create(filePath, FsPermission.getDefault(), 
        false, bufferSize, (short)3, blockSize, 512, null, null, options);
    byte[] buffer = new byte[bufferSize];
    long fileLen = numBlocks * blockSize;
    long left = fileLen;
    while (left > 0) {
      int toWrite = (int) Math.min(bufferSize, left);
      rand.nextBytes(buffer);
      out.write(buffer, 0, toWrite);
      left -= toWrite;
      if (reporter != null) {
        reporter.progress();
      }
    }
    out.close();

    // reduce replication to 1
    fs.setReplication(filePath, (short)1);
    LOG.info("Done creating file");
  }

  //Although the name suggests otherwise the following mapper only creates files of different
  //block sizes.

  public static class CreateFileMapper 
  implements Mapper<Text, LongWritable, Text, Text> {

    private FileSystem fs;
    private int bufferSize;
    private ExecutorService executor = Executors.newFixedThreadPool(10);

    @Override
    public void map(Text key, LongWritable value,
        OutputCollector<Text, Text> output, final Reporter reporter)
            throws IOException {
      String name1 = key.toString();
      List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
      for (int j = 0; j< blockSizes.length;j++) {
        for (int i = 0; i < readTypes.length; i++) {
          String name = getFileNameWithBlockSizeAndReadType(name1, blockSizes[j], readTypes[i]);
          final Path filePath = new Path(DATA_DIR, name);

          final long blockSize = blockSizes[j];
          
          futures.add(executor.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
              createFile(fs, filePath, blockSize, bufferSize, 
                  numblocks, reporter);
              return true;
            }
          }));          
        }
      }

      for (Future future : futures) {
        try {
          future.get();
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }

    @Override
    public void configure(JobConf conf) {
      try {
        fs = FileSystem.get(conf);
      } catch (IOException e) {
        throw new RuntimeException("Can not create file system.", e);
      }
      bufferSize = conf.getInt("dfs.io.file.buffer.size", 4096);
      //blockSizes = new long[] {32 * 1024}; //{32*1024, 4*1024*1024, 64*1024*1024,256*1024*1024};
    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub

    }

  }

  public static void main(String[] args) throws Exception {
    //int res = ToolRunner.run(new ReadBlockBenchMarkCreateFiles(), args);
    int res = new ReadBlockBenchMarkCreateFiles().run(args);
    System.exit(res);
  }
}
