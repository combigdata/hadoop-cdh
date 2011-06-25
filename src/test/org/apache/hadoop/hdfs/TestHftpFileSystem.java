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

package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.Random;

import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Unittest for HftpFileSystem.
 */
public class TestHftpFileSystem {
  private static final Random RAN = new Random();
  private static final Path TEST_FILE = new Path("/testfile1");
  
  private static Configuration config = null;
  private static MiniDFSCluster cluster = null;
  private static FileSystem hdfs = null;
  private static HftpFileSystem hftpFs = null;
    
  /**
   * Setup hadoop mini-cluster for test.
   */
  @BeforeClass
  public static void setUp() throws IOException {
    final long seed = RAN.nextLong();
    System.out.println("seed=" + seed);
    RAN.setSeed(seed);

    config = new Configuration();
    config.set("slave.host.name", "localhost");

    cluster = new MiniDFSCluster(config, 2, true, null);
    hdfs = cluster.getFileSystem();
    final String hftpuri = "hftp://" + config.get("dfs.http.address"); 
    hftpFs = (HftpFileSystem) new Path(hftpuri).getFileSystem(config);
  }
  
  /**
   * Shutdown the hadoop mini-cluster.
   */
  @AfterClass
  public static void tearDown() throws IOException {
    hdfs.close();
    hftpFs.close();
    cluster.shutdown();
  }
  
  /**
   * Tests getPos() functionality.
   */
  @Test
  public void testGetPos() throws Exception {
    // Write a test file.
    FSDataOutputStream out = hdfs.create(TEST_FILE, true);
    out.writeBytes("0123456789");
    out.close();
    
    FSDataInputStream in = hftpFs.open(TEST_FILE);
    
    // Test read().
    for (int i = 0; i < 5; ++i) {
      assertEquals(i, in.getPos());
      in.read();
    }
    
    // Test read(b, off, len).
    assertEquals(5, in.getPos());
    byte[] buffer = new byte[10];
    assertEquals(2, in.read(buffer, 0, 2));
    assertEquals(7, in.getPos());
    
    // Test read(b).
    int bytesRead = in.read(buffer);
    assertEquals(7 + bytesRead, in.getPos());
    
    // Test EOF.
    for (int i = 0; i < 100; ++i) {
      in.read();
    }
    assertEquals(10, in.getPos());
    in.close();
  }
  
  /**
   * Tests seek().
   */
  @Test
  public void testSeek() throws Exception {
    // Write a test file.
    FSDataOutputStream out = hdfs.create(TEST_FILE, true);
    out.writeBytes("0123456789");
    out.close();
    
    FSDataInputStream in = hftpFs.open(TEST_FILE);
    in.seek(7);
    assertEquals('7', in.read());
  }
}
