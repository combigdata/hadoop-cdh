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
package org.apache.hadoop.yarn.server.resourcemanager;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.ZKFailoverController;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.client.RMHAServiceTarget;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RMActiveNodeInfoProto;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;

@InterfaceAudience.Private
public class RMZKFailoverController extends ZKFailoverController {

  private static final Log LOG = LogFactory.getLog(RMZKFailoverController.class);

  private AccessControlList adminAcl;
  private final RMHAServiceTarget localRMServiceTarget;

  @Override
  protected HAServiceTarget dataToTarget(byte[] data) {
    RMActiveNodeInfoProto proto;
    try {
      proto = RMActiveNodeInfoProto.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      throw new YarnRuntimeException(
          "Invalid data in ZK: " + StringUtils.byteToHexString(data));
    }

    // Check if the passed proto corresponds to an RM in the same cluster
    if (!proto.getClusterid().equals(localRMServiceTarget.getClusterId())) {
      throw new YarnRuntimeException("Mismatched cluster! The other RM seems " +
          "to be from a different cluster. Current cluster = " +
          localRMServiceTarget.getClusterId() +
          "Other RM's cluster = " + proto.getClusterid());
    }

    RMHAServiceTarget ret = null;
    try {
      ret = new RMHAServiceTarget((YarnConfiguration)conf, proto.getRmId());
    } catch (IOException e) {
      throw new YarnRuntimeException("Couldn't create RMHAServiceTarget", e);
    }
    ret.setAutoFailoverPort(proto.getZkfcPort());
    return ret;
  }

  @Override
  protected byte[] targetToData(HAServiceTarget target) {
    InetSocketAddress addr = target.getAddress();

    return RMActiveNodeInfoProto.newBuilder()
      .setHostname(addr.getHostName())
      .setPort(addr.getPort())
      .setZkfcPort(target.getZKFCAddress().getPort())
      .setClusterid(localRMServiceTarget.getClusterId())
      .setRmId(localRMServiceTarget.getRmId())
      .build()
      .toByteArray();
  }

  @Override
  protected InetSocketAddress getRpcAddressToBindTo() {
    int zkfcPort = getZkfcPort(conf);
    return new InetSocketAddress(localTarget.getAddress().getAddress(),
          zkfcPort);
  }


  @Override
  protected PolicyProvider getPolicyProvider() {
    return new RMPolicyProvider();
  }

  static int getZkfcPort(Configuration conf) {
    return conf.getInt(YarnConfiguration.RM_HA_AUTOMATIC_FAILOVER_PORT,
        YarnConfiguration.DEFAULT_RM_HA_AUTOMATIC_FAILOVER_PORT);
  }

  public static RMZKFailoverController create(Configuration conf)
      throws IOException {
    YarnConfiguration localRMConf = new YarnConfiguration(conf);
    if (!HAUtil.isHAEnabled(localRMConf)) {
      throw new HadoopIllegalArgumentException(
          "HA is not enabled for this ResourceManager.");
    }

    String rmId = HAUtil.getRMHAId(localRMConf);
    RMHAServiceTarget localTarget = new RMHAServiceTarget(localRMConf, rmId);
    return new RMZKFailoverController(localRMConf, localTarget);
  }

  private RMZKFailoverController(Configuration conf,
                                 RMHAServiceTarget localTarget) {
    super(conf, localTarget);
    this.localRMServiceTarget = localTarget;
    this.adminAcl = new AccessControlList(conf.get(
        YarnConfiguration.YARN_ADMIN_ACL,
        YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));
    LOG.info("Failover controller configured for NameNode " + localTarget);
  }

  @Override
  protected void initRPC() throws IOException {
    super.initRPC();
    localRMServiceTarget.setAutoFailoverPort(rpcServer.getAddress().getPort());
  }

  @Override
  public void loginAsFCUser() throws IOException {
    // No need to do anything in YARN
  }
  
  @Override
  protected String getScopeInsideParentNode() {
    return localRMServiceTarget.getClusterId();
  }

  @Override
  protected void checkRpcAdminAccess() throws IOException {
    try {
      RMServerUtils.verifyAccess(adminAcl, "ZKFC", LOG);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }
}
