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

package org.apache.hadoop.hdfs.server.blockmanagement;

import com.google.common.base.Supplier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;


public class TestNameNodePrunesMissingStorages {
  static final Log LOG = LogFactory.getLog(TestNameNodePrunesMissingStorages.class);

  /**
   * Regression test for HDFS-7960.<p/>
   *
   * Shutting down a datanode, removing a storage directory, and restarting
   * the DataNode should not produce zombie storages.
   */
  @Test(timeout=300000)
  public void testRemovingStorageDoesNotProduceZombies() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 1);
    final int NUM_STORAGES_PER_DN = 2;
    final MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(conf).numDataNodes(3)
        .storagesPerDatanode(NUM_STORAGES_PER_DN)
        .build();
    try {
      cluster.waitActive();
      for (DataNode dn : cluster.getDataNodes()) {
        assertEquals(NUM_STORAGES_PER_DN,
          cluster.getNamesystem().getBlockManager().
              getDatanodeManager().getDatanode(dn.getDatanodeId()).
              getStorageInfos().length);
      }
      // Create a file which will end up on all 3 datanodes.
      final Path TEST_PATH = new Path("/foo1");
      DistributedFileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, TEST_PATH, 1024, (short) 3, 0xcafecafe);
      for (DataNode dn : cluster.getDataNodes()) {
        DataNodeTestUtils.triggerBlockReport(dn);
      }
      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, new Path("/foo1"));
      cluster.getNamesystem().writeLock();
      final String storageIdToRemove;
      String datanodeUuid;
      // Find the first storage which this block is in.
      try {
        Iterator<DatanodeStorageInfo> storageInfoIter =
            cluster.getNamesystem().getBlockManager().
                getStorages(block.getLocalBlock()).iterator();
        assertTrue(storageInfoIter.hasNext());
        DatanodeStorageInfo info = storageInfoIter.next();
        storageIdToRemove = info.getStorageID();
        datanodeUuid = info.getDatanodeDescriptor().getDatanodeUuid();
      } finally {
        cluster.getNamesystem().writeUnlock();
      }
      // Find the DataNode which holds that first storage.
      final DataNode datanodeToRemoveStorageFrom;
      int datanodeToRemoveStorageFromIdx = 0;
      while (true) {
        if (datanodeToRemoveStorageFromIdx >= cluster.getDataNodes().size()) {
          Assert.fail("failed to find datanode with uuid " + datanodeUuid);
          datanodeToRemoveStorageFrom = null;
          break;
        }
        DataNode dn = cluster.getDataNodes().
            get(datanodeToRemoveStorageFromIdx);
        if (dn.getDatanodeUuid().equals(datanodeUuid)) {
          datanodeToRemoveStorageFrom = dn;
          break;
        }
        datanodeToRemoveStorageFromIdx++;
      }
      // Find the volume within the datanode which holds that first storage.
      List<? extends FsVolumeSpi> volumes =
          datanodeToRemoveStorageFrom.getFSDataset().getVolumes();
      assertEquals(NUM_STORAGES_PER_DN, volumes.size());
      String volumeDirectoryToRemove = null;
      for (FsVolumeSpi volume : volumes) {
        if (volume.getStorageID().equals(storageIdToRemove)) {
          volumeDirectoryToRemove = volume.getBasePath();
        }
      }
      // Shut down the datanode and remove the volume.
      // Replace the volume directory with a regular file, which will
      // cause a volume failure.  (If we merely removed the directory,
      // it would be re-initialized with a new storage ID.)
      assertNotNull(volumeDirectoryToRemove);
      datanodeToRemoveStorageFrom.shutdown();
      FileUtil.fullyDelete(new File(volumeDirectoryToRemove));
      FileOutputStream fos = new FileOutputStream(volumeDirectoryToRemove);
      try {
        fos.write(1);
      } finally {
        fos.close();
      }
      cluster.restartDataNode(datanodeToRemoveStorageFromIdx);
      // Wait for the NameNode to remove the storage.
      LOG.info("waiting for the datanode to remove " + storageIdToRemove);
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          final DatanodeDescriptor dnDescriptor =
              cluster.getNamesystem().getBlockManager().getDatanodeManager().
                  getDatanode(datanodeToRemoveStorageFrom.getDatanodeUuid());
          assertNotNull(dnDescriptor);
          DatanodeStorageInfo[] infos = dnDescriptor.getStorageInfos();
          for (DatanodeStorageInfo info : infos) {
            if (info.getStorageID().equals(storageIdToRemove)) {
              LOG.info("Still found storage " + storageIdToRemove + " on " +
                  info + ".");
              return false;
            }
          }
          assertEquals(NUM_STORAGES_PER_DN - 1, infos.length);
          return true;
        }
      }, 10, 30000);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
