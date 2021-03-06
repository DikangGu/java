package org.apache.hadoop.raid.tools;
/*
 * 1. Thread read times
 * 2. Different files for different read types and blocksizes in order to avoid caching between readTypes. 
 * Note that there is no caching problem due to parallel map reduce jobs as they are creating different files.
 * 3. 
 */
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
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ReadOptions;
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

public class ReadBlockBenchMarkReadFiles extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(ReadBlockBenchMarkReadFiles.class);
  private static String TEST_ROOT_DIR = System.getProperty("test.build.data","/benchmarks/ReadBlockBenchMark");
  private static Path CONTROL_DIR = new Path(TEST_ROOT_DIR, "io_control");
  private static Path OUTPUT_DIR = new Path(TEST_ROOT_DIR, "io_output");
  private static Path DATA_DIR = new Path(TEST_ROOT_DIR, "io_data");
  private static final String BASE_FILE_NAME = "test_read_block_";
  
  int numFiles = ReadBlockBenchMarkCreateFiles.numFiles;
  static long[] blockSizes = ReadBlockBenchMarkCreateFiles.blockSizes;
  static String[] readTypes = ReadBlockBenchMarkCreateFiles.readTypes;
  // To be initiated for every run in readFromFile() and to be filled by each ReadWorker thread 
  static long[] threadDurations = null;

	  {
	       Configuration.addDefaultResource("hdfs-default.xml");
	       Configuration.addDefaultResource("hdfs-site.xml");
	       Configuration.addDefaultResource("core-site.xml");
	
	       Configuration.addDefaultResource("raid-default.xml");
	       Configuration.addDefaultResource("raid-site.xml");
	  }

  private void backupOutputDir(FileSystem fs) throws IOException {
    if (fs.exists(OUTPUT_DIR)) {
      Path newPath = new Path(TEST_ROOT_DIR, "io_output_" + System.currentTimeMillis());
      fs.rename(OUTPUT_DIR, newPath);
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
   conf.set("mapred.fairscheduler.pool", "di.sla");
   conf.setBoolean("mapreduce.job.user.classpath.first", true);
    FileSystem fs = FileSystem.get(conf);
    
    //createControlFile(fs, numFiles, conf);
    
    backupOutputDir(fs);
    runBenchmark(OUTPUT_DIR, conf);
    return 0;
  }
  
  
  private static void createControlFile(FileSystem fs, int numFiles, 
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
    return BASE_FILE_NAME + Integer.toString(index);
  }
  
  private static String getFileNameWithBlockSize(String base, long blockSize) {
    return base + '_' + Long.toString(blockSize);
  }
  
  private static String getFileNameWithBlockSizeAndReadType(String base, long blockSize, String readType) {
    return base + '_' + blockSize + '_' + readType;
  }

  
  private static void runBenchmark(Path outputDir, Configuration conf) 
          throws IOException{
    JobConf job = new JobConf(conf, ReadBlockBenchMarkReadFiles.class);
    
    FileInputFormat.setInputPaths(job, CONTROL_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);
    
    job.setMapperClass(ReadBlockMapper.class);
   
    FileOutputFormat.setOutputPath(job, outputDir);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    JobClient.runJob(job);
  }
  
  
  private static long[] readFromFile(FileSystem fs, Path filePath,
      long[] startPos, long[] readLens, Reporter reporter) throws InterruptedException {
    int size = startPos.length;
    Thread[] threads = new Thread[size];
    threadDurations = new long[size];
    for (int i = 0; i < size; i++) {
      threads[i] = new ReadWorker(i, fs, filePath, startPos[i], readLens[i], reporter);
    }
    
    for (int i = 0; i < size; i++) {
      threads[i].start();
    }
    
    for (int i = 0; i < size; i++) {
      threads[i].join();
    }
    
    return(threadDurations);
  }
  
   
  public static class ReadWorker extends Thread {
    int threadID;
    FileSystem fs;
    Path filePath;
    long startPos;
    long readLen;
    Reporter reporter;
    
    int bufferSize = 4096;
    byte[] buffer = new byte[bufferSize];

    public ReadWorker(int threadID, FileSystem fs, Path filePath, long startPos,
        long readLen, Reporter reporter) {
      this.threadID = threadID;
      this.fs = fs;
      this.filePath = filePath;
      this.startPos = startPos;
      this.readLen = readLen;
      this.reporter = reporter;
    }
    
    @Override
    public void run() {
      long startTime = System.currentTimeMillis();
      try {
        DistributedFileSystem dfs = DFSUtil.convertToDFS(fs);
        ReadOptions options = new ReadOptions();
        options.setFadvise(4);
        FSDataInputStream in = dfs.open(filePath, options);
        in.seek(startPos);
        
        long left = readLen;
        while (left > 0) {
          int toRead = (int) Math.min(left, bufferSize);
          toRead = in.read(buffer, 0, toRead);
          if (toRead < 0) {
            break;
          }
          left -= toRead;
          
        }
        in.close();
      } catch (IOException e) {
        throw new RuntimeException("error when reading from file: " + filePath, e);
      } finally {
        threadDurations[threadID] = System.currentTimeMillis() - startTime;
        reporter.progress();
      }
    }
    
  }
  
  private static long readAndReturnReadTime(int m, long blockSize, 
      FileSystem fs, Path filePath, Reporter reporter) throws IOException{
  	long[] startPos = new long[10 + m];
  	long[] readLens = new long[10 + m];
  	
  	for (int i = 0; i < (10+m); i++) {
  		startPos[i] = i * blockSize;
  		readLens[i] = Math.round((double) (10.0)*blockSize/(10 + m));      		      		
  	}
  	long startTime = System.currentTimeMillis();
  	try {
  		readFromFile(fs, filePath, startPos, readLens, reporter);
  	} catch (InterruptedException e) {
  		throw new InterruptedIOException();
  	}
  	long totalTime = System.currentTimeMillis() - startTime;
  	return(totalTime);
  }
  
  public static class ReadBlockMapper 
      implements Mapper<Text, LongWritable, Text, Text> {
    
    private FileSystem fs;
    private int bufferSize;
    private long fileLen;
    
    private long calculateReadTime(int type) {
      long startTime = 0;
      return 0;
    }
    
    @Override
    public void map(Text key, LongWritable value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      String name1 = key.toString();
      
      for (int j = 0; j< blockSizes.length;j++) {
                
      	long blockSize = blockSizes[j];
      	//since creating is already done using another mapper
      	/* createFile(fs, new Path(name), blockSize, bufferSize, 
          fileLen, reporter);
      	 */
      	
      	String timeResult = "";
      	
      	// read full blockSize from 10 blocks;
      	String name = getFileNameWithBlockSizeAndReadType(name1, blockSizes[j],readTypes[0]);
      	Path filePath = new Path(DATA_DIR, name);
      	long[] startPos = new long[10];
      	long[] readLens = new long[10];
      	long[] threadReadTimes = new long[10];
      	for (int i = 0; i < 10; i++) {
      		startPos[i] = i * blockSize;
      		readLens[i] = blockSize;
      	}

      	long startTime = System.currentTimeMillis();
      	try {
      		threadReadTimes = readFromFile(fs, filePath, startPos, readLens, reporter);
      	} catch (InterruptedException e) {
      		throw new InterruptedIOException();
      	}
      	long totalTime = System.currentTimeMillis() - startTime;
      	
      	//read 2 full blocks and 9 half blocks
      	name = getFileNameWithBlockSizeAndReadType(name1, blockSizes[j],readTypes[1]);
      	filePath = new Path(DATA_DIR, name);
      	long[] startPos2 = new long[11];
      	long[] readLens2 = new long[11];
      	long[] threadReadTimes2 = new long[11];
      	
      	for (int i = 0; i < 11; i++) {
      		startPos2[i] = i * blockSize;
      		if (i < 2) {
      			readLens2[i] = blockSize;      			
      		} else {
      			readLens2[i] = (long) Math.round((double) blockSize/2.0);
      		}      		
      	}
      	startTime = System.currentTimeMillis();
      	try {
      		threadReadTimes2 = readFromFile(fs, filePath, startPos2, readLens2, reporter);
      	} catch (InterruptedException e) {
      		throw new InterruptedIOException();
      	}
      	long totalTime2 = System.currentTimeMillis() - startTime;
      	
      	//read 13 half blocks (total = 6.5*blockSize)
      	name = getFileNameWithBlockSizeAndReadType(name1, blockSizes[j],readTypes[2]);
      	filePath = new Path(DATA_DIR, name);
      	long[] startPos3 = new long[13];
      	long[] readLens3 = new long[13];
      	long[] threadReadTimes3 = new long[13];
      	for (int i = 0; i < 13; i++) {
      		startPos3[i] = i * blockSize;
      		readLens3[i] = (long) Math.round((double) blockSize/2.0);      		      		
      	}
      	startTime = System.currentTimeMillis();
      	try {
      		threadReadTimes3 = readFromFile(fs, filePath, startPos3, readLens3, reporter);
      	} catch (InterruptedException e) {
      		throw new InterruptedIOException();
      	}
      	long totalTime3 = System.currentTimeMillis() - startTime;
      	
      	/*
      	//read (10/m)*blockSize from m nodes for m= 11,12,13
      	long[] totalTimes4 = new long[3];
      	for (int k = 1; k < 4; k++) {
      		name = getFileNameWithBlockSizeAndReadType(name1, blockSizes[j],readTypes[2+k]);
        	filePath = new Path(DATA_DIR, name);
      		//(int m, long blockSize, FileSystem fs, String name)
        	totalTimes4[k - 1] = readAndReturnReadTime(k, blockSize, fs, filePath, reporter);
      	}
      	
      	timeResult = String.valueOf(totalTime) + ", " + String.valueOf(totalTime2) + ", "
      	    + String.valueOf(totalTime3) + ", " + String.valueOf(totalTimes4[0]) + ", " 
      	    + String.valueOf(totalTimes4[1]) + ", " + String.valueOf(totalTimes4[2]);
      	*/
      	timeResult = String.valueOf(totalTime) + ", " + String.valueOf(totalTime2) + ", "
      	    + String.valueOf(totalTime3);
      	for (int i = 0; i< 10; i++) {
      		timeResult = timeResult + ", " + String.valueOf(threadReadTimes[i]);
      	}
      	
      	for (int i = 0; i< 11; i++) {
      		timeResult = timeResult + ", " + String.valueOf(threadReadTimes2[i]);
      	}
      	for (int i = 0; i< 13; i++) {
      		timeResult = timeResult + ", " + String.valueOf(threadReadTimes3[i]);
      	}
      	    
      	output.collect(new Text(name), new Text(timeResult));
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
    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub
      
    }
    
  }
  
  public static void main(String[] args) throws Exception {
    int res = new ReadBlockBenchMarkReadFiles().run(args);
    //int res = ToolRunner.run(new ReadBlockBenchMarkReadFiles(), args);
    System.exit(res);
  }
}
