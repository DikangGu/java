package org.apache.hadoop.raid.tools;

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
import org.apache.hadoop.raid.*;

public class RS_PbRS_ComputeTimeComparison {
  private static final Log LOG = LogFactory.getLog(RS_PbRS_ComputeTimeComparison.class);
  private static String TEST_ROOT_DIR = System.getProperty("test.build.data","/benchmarks/RS_PbRS_ComputeTimeComparison");
  private static Path CONTROL_DIR = new Path(TEST_ROOT_DIR, "io_control");
  private static Path OUTPUT_DIR = new Path(TEST_ROOT_DIR, "io_output");
  private static final String BASE_FILE_NAME = "test_compute_time_";
  
  public static int numFiles = 100;
  
    {
       Configuration.addDefaultResource("hdfs-default.xml");
       Configuration.addDefaultResource("hdfs-site.xml"); 
       Configuration.addDefaultResource("core-site.xml"); 
        
       Configuration.addDefaultResource("raid-default.xml");
       Configuration.addDefaultResource("raid-site.xml");
    }

  public int run(String[] args) throws Exception {    
	  Configuration conf = new Configuration();
    conf.set("mapred.fairscheduler.pool", "di.sla");
    conf.setBoolean("mapreduce.job.user.classpath.first", true);
    FileSystem fs = FileSystem.get(conf);
    
    createControlFiles(fs, numFiles, conf);
    
    fs.delete(OUTPUT_DIR, true);
		runEncodeTimeComparison(OUTPUT_DIR, conf);
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
	
	private static void runEncodeTimeComparison(Path outputDir, Configuration conf)
			throws IOException {
    JobConf job = new JobConf(conf, RS_PbRS_ComputeTimeComparison.class);
    
    FileInputFormat.setInputPaths(job, CONTROL_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);
    
    job.setMapperClass(RS_PbRS_ComputeTimeComparisonMapper.class);
   
    FileOutputFormat.setOutputPath(job, outputDir);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    JobClient.runJob(job);
		
	}
		public static class RS_PbRS_ComputeTimeComparisonMapper 
  implements Mapper<Text, LongWritable, Text, Text> {
		private int bufferSize;    
    //private int blockSize; // not using int since upto 256M this is fine
    private int stripeSize;
    private int paritySize;
    private int subPacketization;
    private byte [][][] readBuffsPB;
    private byte [][][] writeBuffsPB;
    private byte [][] readBuffsRS;
    private byte [][] writeBuffsRS;
    private int erasedLocationToFix; 
    private Random RAND; 
    
    //both the two below locations are 0,.., (paritySize+stripeSize-1) with 
    //parity locations coming first.
    private int[][] locConsiderErased; //for PB-RS based on what is read
    private int[] erasedLocs; //for RS based on what is read
    private int bufSize; 
    private int bufSizePerSubpkt;
    private int dataStart; 
    private int dataLenForPB;
    private int dataLenForRS;
    private boolean xorVersion;
    
    private byte[][] readBufsForEncRS;
    private byte[][] writeBufsForEncRS;
    private byte[][][] readBufsForEncPB;
    private byte[][][] writeBufsForEncPB;
    private PiggyBackCode pbcode;
    private PiggyBackDecoder pbdecoder; //just to use the method getLocConsideredErased
    private ReedSolomonCode rscode;
    
    @Override
    public void map(Text key, LongWritable value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      String name1 = key.toString(); 
      String timeResult = "";
      
      //first perform RS encoding and measure time
      readBufsForEncRS =  returnRandomDataForEncodeRS();
      long startTime = System.currentTimeMillis();
      rscode.encodeBulk(readBufsForEncRS, writeBufsForEncRS);
      long timeComputeEncodeRS = System.currentTimeMillis() - startTime;
      
      //then perform PB-RS encoding and measure time
      readBufsForEncPB = returnRandomDataForEncodePB();
      startTime = System.currentTimeMillis();
      pbcode.jointEncodeSubPackets(readBufsForEncPB, readBufsForEncPB, bufSizePerSubpkt);
      long timeComputeEncodePB1 = System.currentTimeMillis() - startTime;
      
      startTime = System.currentTimeMillis();
      xorVersion = false;
      pbcode.jointEncodeSubPacketsNew(readBufsForEncPB, writeBufsForEncPB, bufSizePerSubpkt, xorVersion);
      long timeComputeEncodePB2 = System.currentTimeMillis() - startTime;
      
      //first perform RS decoding and measure time      
      readBuffsRS = returnRandomDataForDecodeRS();
      startTime = System.currentTimeMillis();
      rscode.decodeBulk(readBuffsRS, writeBuffsRS, erasedLocs, dataStart, dataLenForRS);
      long timeComputeDecodeRS = System.currentTimeMillis() - startTime;
      
      //second perform PB-RS decoding and measure time      
      readBuffsPB = returnRandomDataForDecodePB();
      
      //first type of PB-RS decoding: first do RS decoding on second subpkt, get the piggyback,
      //and then do RS decoding again to obtain missing data in first subpkt.
      startTime = System.currentTimeMillis();
      pbcode.jointDecodeDataSubPackets(readBuffsPB, writeBuffsPB, 
      		erasedLocationToFix, locConsiderErased, bufSizePerSubpkt, dataStart, dataLenForPB);
      long timeComputeDecodePB1 = System.currentTimeMillis() - startTime;
      
      //second type of PB-RS decoding: first do RS decoding for the second sub-packet, obtain the piggyback
      // by subtracting, and then use the encoding vector to perform elementary field operations to recover the
      // missing data in the first sub-packet.
      startTime = System.currentTimeMillis();
      pbcode.jointDecodeDataSubPacketsGenMx(readBuffsPB, writeBuffsPB, 
      		erasedLocationToFix, locConsiderErased, bufSizePerSubpkt, dataStart, dataLenForPB);
      long timeComputeDecodePB2 = System.currentTimeMillis() - startTime;
      
      //Reconstruct missing data bytes: by using RS decoding for the second sub-packet, obtain the piggyback
      //by subtracting, and then just do XOR to recover the missing data in the first sub-packet.
      startTime = System.currentTimeMillis();
      pbcode.jointDecodeDataSubPacketsXOR(readBuffsPB, writeBuffsPB, 
      		erasedLocationToFix, locConsiderErased, bufSizePerSubpkt, dataStart, dataLenForPB);
      long timeComputeDecodePB3 = System.currentTimeMillis() - startTime;    
      
      timeResult = String.valueOf(timeComputeEncodeRS) + ", " + String.valueOf(timeComputeEncodePB1) + ", "
      		+ String.valueOf(timeComputeEncodePB2); 
      timeResult = timeResult + String.valueOf(timeComputeDecodeRS) + ", " + String.valueOf(timeComputeDecodePB1) + ", "
      		+ String.valueOf(timeComputeDecodePB2) + ", " + String.valueOf(timeComputeDecodePB3);
      
      //Please check if it is okay to use name1 as key in the below line
      output.collect(new Text(name1), new Text(timeResult));
      
    }
      
    @Override
    public void configure(JobConf conf) {      
      bufferSize = 1024*1024;
      		//conf.getInt("test.io.file.buffer.size", 1024*1024);
      //note: block size is not used anywhere since for computation only need to evaluate at 
      //buffersize
      //blockSize = 64*1024*1024; //256M /4 (since parallelism = 4 is used. Try with 
      //different block sizes to see the impact (if any) of block size on computation time.)
      RAND = new Random();
      stripeSize =  10;
      paritySize = 4;
      subPacketization = 2;
      xorVersion = false;
      pbcode = new PiggyBackCode(stripeSize, paritySize);
      rscode = new ReedSolomonCode(stripeSize, paritySize);
    //xorVersion value does not matter for getLocconsideredErased method
      pbdecoder = new PiggyBackDecoder(conf, xorVersion);
      erasedLocationToFix = paritySize; //first data block
      try {
      	locConsiderErased = pbdecoder.getLocConsiderErased(erasedLocationToFix, true);
      } catch (TooManyErasedLocations TMEL) {
      	LOG.info("Exception: too many erased locations.");
      }      
      bufSize = bufferSize;
      bufSizePerSubpkt = (int) Math.ceil(bufSize * 1.0 / subPacketization);
      dataStart = 0;
      dataLenForPB = (int) Math.ceil(bufSize * 1.0 / subPacketization); 
      dataLenForRS = bufSize;     
      readBuffsPB = new byte[2][paritySize + stripeSize][bufSizePerSubpkt];
      readBuffsRS = new byte[paritySize + stripeSize][bufSize];
      erasedLocs = new int[]{1,2,3,erasedLocationToFix}; //for RS      
      writeBuffsPB = new byte[2][paritySize][bufSizePerSubpkt];
      writeBuffsRS = new byte[paritySize][bufSize];
      
      readBufsForEncPB = new byte[2][stripeSize][bufSizePerSubpkt];
      readBufsForEncRS = new byte[stripeSize][bufSize];
      writeBufsForEncRS = new byte[paritySize][bufSize];
      writeBufsForEncPB = new byte[2][paritySize][bufSizePerSubpkt];      		
    }
    
    /*
     * Returns random data for reconstruction of any data location.
     * Uses pbdecoder's datadecodedatareadloc and datadecodeparityreadloc
     */
    
    private byte[][][] returnRandomDataForDecodePB() {
    	//
    	byte[][][] randomData = new byte[subPacketization][paritySize + stripeSize][bufSizePerSubpkt];
    	
    	//parity read locations
    	for (int i = 0; i < paritySize; i++) {
    		for (int j = 0; j < subPacketization; j++) {
    			if (pbdecoder.dataDecodeParityReadLoc[erasedLocationToFix-paritySize][i][j]) {
    				RAND.nextBytes(randomData[j][i]);
    			}
    		}
    	}
    	
    	//data read locations
    	for (int i = 0; i < stripeSize; i++) {
    		for (int j = 0; j < subPacketization; j++) {
    			if (pbdecoder.dataDecodeDataReadLoc[erasedLocationToFix-paritySize][i][j]) {
    				RAND.nextBytes(randomData[j][paritySize + i]);
    			}
    		}
    	}
    	
    	return(randomData);
    }
    
    private byte[][] returnRandomDataForDecodeRS() {
    	byte[][] randomData = new byte[paritySize + stripeSize][bufSize];
    	//hard coding that first parity location is used
    	RAND.nextBytes(randomData[0]);
    	
    	//erased location = paritySize so not filling it with data
    	for (int i = paritySize+1; i < paritySize + stripeSize; i++) {
    		RAND.nextBytes(randomData[i]);
      
    	}
    	return(randomData);
    }
    
    private byte[][][] returnRandomDataForEncodePB() {
    	//use pbdecoder datadecodedatareadloc and datadecodeparityreadloc
    	byte[][][] randomData = new byte[2][stripeSize][bufSizePerSubpkt];
    	for( int j = 0; j < subPacketization; j++) {
    		for (int i = 0; i < stripeSize; i++) {
    			RAND.nextBytes(randomData[j][i]);
    		}
    	}
    	return(randomData);
    }
    
    private byte[][] returnRandomDataForEncodeRS() {
    	//
    	byte[][] randomData = new byte[stripeSize][bufSize];
    	//int symbolMax = (int) Math.pow(2, rscode.symbolSize());
    	for (int i = 0; i < stripeSize; i++) {
    		RAND.nextBytes(randomData[i]);
    	}    	
    	return(randomData);
    }
    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub
      
    }
	}

    public static void main(String[] argv) throws Exception {
      int res = new RS_PbRS_ComputeTimeComparison().run(argv);
     System.exit(res);
    }

}
