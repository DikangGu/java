/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestBlockFileChannelCache {
  public static final Log LOG = LogFactory.getLog(TestBlockFileChannelCache.class);
  {
    DataNode.LOG.getLogger().setLevel(Level.ALL);
  }
  
  private MiniDFSCluster cluster = null;
  private DistributedFileSystem dfs = null;
  private Configuration conf = null;
  
  private static long DEFAULT_BLOCK_SIZE = 1024;
  private int cacheSize = 2;
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    conf.setInt(DataNode.DFS_DATANODE_FILE_HANDLER_CACHE_SIZE_KEY, cacheSize);
    
    cluster = new MiniDFSCluster(conf, 1, true, null);
    dfs = DFSUtil.convertToDFS(cluster.getFileSystem());
  }
  
  @After
  public void tearDown() throws Exception {
    IOUtils.cleanup(LOG, dfs, cluster);
  }
  
  private File getBlockFile(Block block) throws IOException {
    return getBlockFile(block, 0);
  }
  
  private File getBlockFile(Block block, int replica) throws IOException {
    for (int i = replica*2; i < replica*2 + 2; i++) {
      File blockFile = new File(cluster.getBlockDirectory("data" + (i+1)), block.getBlockName());
      if (blockFile.exists()) {
        return blockFile;
      }
      File blockFileInlineChecksum = new File(cluster.getBlockDirectory("data" + (i+1)), 
          BlockInlineChecksumWriter.getInlineChecksumFileName(
          block, FSConstants.CHECKSUM_TYPE, conf.getInt(
              "io.bytes.per.checksum", FSConstants.DEFAULT_BYTES_PER_CHECKSUM)));
      if (blockFileInlineChecksum.exists()) {
        return blockFileInlineChecksum;
      }
    }
    
    return null;
  }
  
  @Test
  public void testCache() throws IOException {
    Path fileName = new Path("/user/facebook/file1");
    int numBlocks = 5;
    DFSTestUtil.createFile(dfs, fileName, 
        numBlocks * DEFAULT_BLOCK_SIZE, (short)1, 1);
    
    LocatedBlocks locatedBlocks = dfs.getClient().getLocatedBlocks(fileName.toString(), 
        0, Long.MAX_VALUE);
    List<LocatedBlock> blockList = locatedBlocks.getLocatedBlocks();
    DataNode datanode = cluster.getDataNodes().get(0);
    
    byte[] buffer = new byte[(int)DEFAULT_BLOCK_SIZE];
    // read one block
    FSDataInputStream in = dfs.open(fileName);
    in.read(buffer);
    assertEquals(1, datanode.fileHandlerCache.size());
    File firstBlock = getBlockFile(blockList.get(0).getBlock());
    assertTrue(datanode.fileHandlerCache.contains(firstBlock));
    
    // read another block
    in.read(buffer);
    assertEquals(2, datanode.fileHandlerCache.size());
    File secondBlock = getBlockFile(blockList.get(1).getBlock());
    assertTrue(datanode.fileHandlerCache.contains(firstBlock));
    assertTrue(datanode.fileHandlerCache.contains(secondBlock));
    
    // read more blocks
    in.read(buffer);
    assertEquals(2, datanode.fileHandlerCache.size());
    File thirdBlock = getBlockFile(blockList.get(2).getBlock());
    assertTrue(datanode.fileHandlerCache.contains(thirdBlock));
    assertFalse(datanode.fileHandlerCache.contains(firstBlock));
    
    in.read(buffer);
    assertEquals(2, datanode.fileHandlerCache.size());
    File fourthBlock = getBlockFile(blockList.get(3).getBlock());
    assertTrue(datanode.fileHandlerCache.contains(fourthBlock));
    assertFalse(datanode.fileHandlerCache.contains(secondBlock));
    
    // last block
    in.read(buffer);
    assertEquals(2, datanode.fileHandlerCache.size());
    File lastBlock = getBlockFile(blockList.get(4).getBlock());
    assertTrue(datanode.fileHandlerCache.contains(lastBlock));
    
    in.close();
    assertEquals(2, datanode.fileHandlerCache.size());
    assertTrue(datanode.fileHandlerCache.contains(fourthBlock));
    assertTrue(datanode.fileHandlerCache.contains(lastBlock));
    LOG.info("testCache finished.");
  }
  
  private void createFile(Path filePath, byte[] buffer) throws IOException {
    OutputStream out = dfs.create(filePath, (short)1);
    out.write(buffer);
    out.close();
  }
  
  @Test
  public void testMultiThread() throws Exception {
    Path fileName = new Path("/user/facebook/file2");
    int numBlocks = 8;
    //DFSTestUtil.createFile(dfs, fileName, 
    //    numBlocks * DEFAULT_BLOCK_SIZE, (short)1, 1);
    byte[] buffer = new byte[(int) (numBlocks * DEFAULT_BLOCK_SIZE)];
    Random random = new Random();
    random.nextBytes(buffer);
    createFile(fileName, buffer);
    
    DataNode datanode = cluster.getDataNodes().get(0);
    
    assertEquals(0, datanode.fileHandlerCache.size());
    int numThreads = 20;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
    for (int i = 0; i < numThreads; i++) {
      futures.add(executor.submit(new ReadWorker(fileName, dfs, buffer)));
    }
    
    for (int i = 0; i < numThreads; i++) {
      assertTrue(futures.get(i).get());
    }
    assertEquals(2, datanode.fileHandlerCache.size());
    LOG.info("testMultiThread finished.");
  }
  
  public static class ReadWorker implements Callable<Boolean> {
    private Path filePath;
    private DistributedFileSystem dfs;
    private Random random;
    private byte[] content;
    
    public ReadWorker(Path filePath, DistributedFileSystem dfs, byte[] buffer) {
      this.filePath = filePath;
      this.dfs = dfs;
      random = new Random();
      this.content = buffer;
    }
    
    private int generateLen(int fileLen) {
      int len = random.nextInt(fileLen / 2);
      while (len  < (int)DEFAULT_BLOCK_SIZE) {
        len = random.nextInt(fileLen / 2);
      }
      return len;
    }
    
    @Override
    public Boolean call() throws Exception {
      FileStatus status = dfs.getFileStatus(filePath);
      int fileLen = (int) status.getLen();
      
      for (int i = 0; i < 10; i++) {
        int startPos = random.nextInt(fileLen / 2);
        int readLen = generateLen(fileLen);
        if (!read(startPos, readLen)) {
          return false;
        }
        Thread.sleep(10);
      }
      
      return true;
    }
    
    private boolean read(int startPos, int readLen) throws IOException {
      byte[] buffer = new byte[readLen];
      FSDataInputStream in = dfs.open(filePath);
      in.seek(startPos);
      int offset = 0;
      while (offset < readLen) {
        int numRead = in.read(buffer, offset, readLen - offset);
        offset += numRead;
      }
      in.close();
      for (int i = 0; i < readLen; i++) {
        if (content[i + startPos] != buffer[i]) {
          LOG.error("Dikang: data loss!!!");
          return false;
        }
      }
      return true;
    }
    
  }
}
