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

package org.apache.hadoop.yarn.client;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ha.BadFencingConfigurationException;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.NodeFencer;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;
import java.net.InetSocketAddress;

public class RMHAServiceTarget extends HAServiceTarget {
  private NodeFencer fencer;
  private InetSocketAddress haAdminServiceAddress;
  private InetSocketAddress failoverControllerAddr;
  private String clusterId;
  private String rmId;
  private boolean autoFailoverEnabled = false;

  public RMHAServiceTarget(YarnConfiguration conf, String nodeId)
      throws IOException {
    this.rmId = nodeId;
    conf.set(YarnConfiguration.RM_HA_ID, nodeId);
    this.clusterId = conf.get(YarnConfiguration.RM_CLUSTER_ID,
        YarnConfiguration.DEFAULT_RM_CLUSTER_ID);

    haAdminServiceAddress = conf.getSocketAddr(
        YarnConfiguration.RM_HA_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_HA_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_HA_ADMIN_PORT);

    if (conf.get(YarnConfiguration.RM_HA_FENCER) == null) {
      conf.set(YarnConfiguration.RM_HA_FENCER,
          YarnConfiguration.DEFAULT_RM_HA_FENCER);
    }
    fencer = NodeFencer.create(conf, YarnConfiguration.RM_HA_FENCER);

    if (HAUtil.isAutomaticFailoverEnabled(conf)) {
      autoFailoverEnabled = true;
      int port = conf.getInt(YarnConfiguration.RM_HA_AUTOMATIC_FAILOVER_PORT,
          YarnConfiguration.DEFAULT_RM_HA_AUTOMATIC_FAILOVER_PORT);
      if (port != 0) {
        setAutoFailoverPort(port);
      }
    }
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getRmId() {
    return rmId;
  }

  @Override
  public boolean isAutoFailoverEnabled() {
    return autoFailoverEnabled;
  }

  @Override
  public InetSocketAddress getAddress() {
    return haAdminServiceAddress;
  }

  @Override
  public InetSocketAddress getZKFCAddress() {
    return failoverControllerAddr;
  }

  @Override
  public NodeFencer getFencer() {
    return fencer;
  }

  @Override
  public void checkFencingConfigured()
      throws BadFencingConfigurationException {
    // do nothing - fencing is implicit
  }

  public void setAutoFailoverPort(int port) {
    Preconditions.checkState(autoFailoverEnabled,
        "ZKFC address not relevant when auto failover is off");
    this.failoverControllerAddr =
        new InetSocketAddress(haAdminServiceAddress.getAddress(), port);
  }

}
