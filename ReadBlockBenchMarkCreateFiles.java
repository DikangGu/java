package org.apache.hadoop.raid;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
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
  
  public static int numFiles = 10;
  public static final long[] blockSizes = new long[] {32*1024, 4*1024*1024, 64*1024*1024,256*1024*1024};
  
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration(getConf());
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
    OutputStream out = fs.create(filePath, true, bufferSize, (short)3, blockSize);
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
  }
  
  //Although the name suggests otherwise the following mapper only creates files of different
  //block sizes.
  
  public static class CreateFileMapper 
      implements Mapper<Text, LongWritable, Text, Text> {
    
    private FileSystem fs;
    private int bufferSize;
    
    @Override
    public void map(Text key, LongWritable value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
    	String name1 = key.toString();
    	for (int j = 0; j< blockSizes.length;j++) {
    		String name = getFileNameWithBlockSize(name1, blockSizes[j]);
    		Path filePath = new Path(DATA_DIR, name);
    		createFile(fs, filePath, blockSizes[j], bufferSize, 
    				20, reporter);
    	}
    }

    @Override
    public void configure(JobConf conf) {
      try {
        fs = FileSystem.get(conf);
      } catch (IOException e) {
        throw new RuntimeException("Can not create file system.", e);
      }
      bufferSize = conf.getInt("test.io.file.buffer.size", 4096);
      //blockSizes = new long[] {32 * 1024}; //{32*1024, 4*1024*1024, 64*1024*1024,256*1024*1024};
    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub
      
    }
    
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ReadBlockBenchMarkCreateFiles(), args);
    System.exit(res);
  }
}
