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

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FsTracer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.htrace.core.SpanId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;


public class TestDFSOutputStream {
  static MiniDFSCluster cluster;

  @BeforeClass
  public static void setup() throws IOException {
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).build();
  }

  /**
   * The close() method of DFSOutputStream should never throw the same exception
   * twice. See HDFS-5335 for details.
   */
  @Test
  public void testCloseTwice() throws IOException {
    DistributedFileSystem fs = cluster.getFileSystem();
    FSDataOutputStream os = fs.create(new Path("/test"));
    DFSOutputStream dos = (DFSOutputStream) Whitebox.getInternalState(os,
        "wrappedStream");
    @SuppressWarnings("unchecked")
    AtomicReference<IOException> ex = (AtomicReference<IOException>) Whitebox
        .getInternalState(dos, "lastException");
    Assert.assertEquals(null, ex.get());

    dos.close();

    IOException dummy = new IOException("dummy");
    ex.set(dummy);
    try {
      dos.close();
    } catch (IOException e) {
      Assert.assertEquals(e, dummy);
    }
    Assert.assertEquals(null, ex.get());
    dos.close();
  }

  @Test
  public void testCongestionBackoff() throws IOException {
    DFSClient.Conf dfsClientConf = mock(DFSClient.Conf.class);
    DFSClient client = mock(DFSClient.class);
    when(client.getConf()).thenReturn(dfsClientConf);
    client.clientRunning = true;
    DistributedFileSystem fs = cluster.getFileSystem();
    FSDataOutputStream os = fs.create(new Path("/foo"));
    DFSOutputStream dos = (DFSOutputStream) Whitebox.getInternalState(os,
        "wrappedStream");
    DFSOutputStream.DataStreamer stream = (DFSOutputStream.DataStreamer)
        Whitebox.getInternalState(dos, "streamer");

    DataOutputStream blockStream = mock(DataOutputStream.class);
    doThrow(new IOException()).when(blockStream).flush();
    Whitebox.setInternalState(stream, "blockStream", blockStream);
    Whitebox.setInternalState(stream, "stage",
                              BlockConstructionStage.PIPELINE_CLOSE);
    @SuppressWarnings("unchecked")
    LinkedList<DFSOutputStream.Packet> dataQueue = (LinkedList<DFSOutputStream.Packet>)
        Whitebox.getInternalState(dos, "dataQueue");
    @SuppressWarnings("unchecked")
    ArrayList<DatanodeInfo> congestedNodes = (ArrayList<DatanodeInfo>)
        Whitebox.getInternalState(stream, "congestedNodes");
    congestedNodes.add(mock(DatanodeInfo.class));
    DFSOutputStream.Packet packet = mock(DFSOutputStream.Packet.class);
    when(packet.getTraceParents()).thenReturn(new SpanId[] {});
    dataQueue.add(packet);
    stream.run();
    Assert.assertTrue(congestedNodes.isEmpty());
  }

  @AfterClass
  public static void tearDown() {
    cluster.shutdown();
  }
}
