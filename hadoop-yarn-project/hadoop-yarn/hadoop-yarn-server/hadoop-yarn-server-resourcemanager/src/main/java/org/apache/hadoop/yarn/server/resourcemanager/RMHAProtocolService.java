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

import com.google.common.annotations.VisibleForTesting;

import com.google.protobuf.BlockingService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownThreadsHelper;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;

import java.io.IOException;
import java.net.InetSocketAddress;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RMHAProtocolService extends AbstractService implements
    HAServiceProtocol {
  private static final Log LOG = LogFactory.getLog(RMHAProtocolService.class);

  private Configuration conf;
  private ResourceManager rm;
  @VisibleForTesting
  protected HAServiceState haState = HAServiceState.INITIALIZING;
  private AccessControlList adminAcl;
  private Server haAdminServer;
  private Thread failoverThread;

  private boolean haEnabled;
  private boolean autoFailoverEnabled;

  public RMHAProtocolService(ResourceManager resourceManager)  {
    super("RMHAProtocolService");
    this.rm = resourceManager;
  }

  @Override
  protected synchronized void serviceInit(Configuration conf) throws
      Exception {
    this.conf = conf;
    adminAcl = new AccessControlList(conf.get(
        YarnConfiguration.YARN_ADMIN_ACL,
        YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));

    haEnabled = HAUtil.isHAEnabled(this.conf);
    if (haEnabled) {
      HAUtil.setAllRpcAddresses(this.conf);
      rm.setConf(this.conf);
      autoFailoverEnabled = HAUtil.isAutomaticFailoverEnabled(this.conf);
      if (autoFailoverEnabled) {
        createFailoverThread();
      }
    }
    rm.createAndInitActiveServices();
    super.serviceInit(this.conf);
  }

  @Override
  protected synchronized void serviceStart() throws Exception {
    if (haEnabled) {
      transitionToStandby(true);
      startHAAdminServer();
      if (autoFailoverEnabled) {
        failoverThread.start();
      }
    } else {
      transitionToActive();
    }

    super.serviceStart();
  }

  @Override
  protected synchronized void serviceStop() throws Exception {
    if (haEnabled) {
      if (autoFailoverEnabled) {
        ShutdownThreadsHelper.shutdownThread(failoverThread, 3000);
      }
      stopHAAdminServer();
    }
    transitionToStandby(false);
    haState = HAServiceState.STOPPING;
    super.serviceStop();
  }

  protected void startHAAdminServer() throws Exception {
    InetSocketAddress haAdminServiceAddress = conf.getSocketAddr(
        YarnConfiguration.RM_HA_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_HA_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_HA_ADMIN_PORT);

    RPC.setProtocolEngine(conf, HAServiceProtocolPB.class,
        ProtobufRpcEngine.class);

    HAServiceProtocolServerSideTranslatorPB haServiceProtocolXlator =
        new HAServiceProtocolServerSideTranslatorPB(this);
    BlockingService haPbService =
        HAServiceProtocolProtos.HAServiceProtocolService
            .newReflectiveBlockingService(haServiceProtocolXlator);

    WritableRpcEngine.ensureInitialized();

    String bindHost = haAdminServiceAddress.getHostName();

    int serviceHandlerCount = conf.getInt(
        YarnConfiguration.RM_HA_ADMIN_CLIENT_THREAD_COUNT,
        YarnConfiguration.DEFAULT_RM_HA_ADMIN_CLIENT_THREAD_COUNT);

    haAdminServer = new RPC.Builder(conf)
        .setProtocol(HAServiceProtocolPB.class)
        .setInstance(haPbService)
        .setBindAddress(bindHost)
        .setPort(haAdminServiceAddress.getPort())
        .setNumHandlers(serviceHandlerCount)
        .setVerbose(false)
        .build();

    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
      haAdminServer.refreshServiceAcl(conf, new RMPolicyProvider());
    }

    haAdminServer.start();
    conf.updateConnectAddr(YarnConfiguration.RM_HA_ADMIN_ADDRESS,
        haAdminServer.getListenerAddress());
  }

  private void stopHAAdminServer() throws Exception {
    if (haAdminServer != null) {
      haAdminServer.stop();
      haAdminServer.join();
      haAdminServer = null;
    }
  }

  private void createFailoverThread() throws ClassNotFoundException {
    Class<? extends RMFailoverController> defaultFailoverController;
    try {
      defaultFailoverController =
          (Class<? extends RMFailoverController>) Class.forName(
              YarnConfiguration.DEFAULT_RM_HA_AUTOMATIC_FAILOVER_CONTROLLER);
    } catch (ClassCastException cce) {
      throw new YarnRuntimeException("The failover controller plugin " +
          "provided does not implement " + RMFailoverController.class);
    }

    RMFailoverController failoverController = ReflectionUtils.newInstance(
        conf.getClass(YarnConfiguration.RM_HA_AUTOMATIC_FAILOVER_CONTROLLER,
            defaultFailoverController, RMFailoverController.class), conf);
    failoverThread = new Thread(failoverController, "FailoverController");
    failoverThread.setDaemon(true);
  }

  @Override
  public synchronized void monitorHealth()
      throws HealthCheckFailedException, AccessControlException {
    checkAccess("monitorHealth");
    if (haState == HAServiceState.ACTIVE && !rm.areActiveServicesRunning()) {
      throw new HealthCheckFailedException(
          "Active ResourceManager services are not running!");
    }
  }

  private synchronized void transitionToActive() throws Exception {
    if (haState == HAServiceState.ACTIVE) {
      LOG.info("Already in active state");
      return;
    }

    LOG.info("Transitioning to active");
    rm.startActiveServices();
    haState = HAServiceState.ACTIVE;
    LOG.info("Transitioned to active");
  }

  @Override
  public synchronized void transitionToActive(StateChangeRequestInfo reqInfo)
      throws AccessControlException {
    checkAccess("transitionToActive");
    checkTransitionAllowed(reqInfo);
    try {
      transitionToActive();
    } catch (Exception e) {
      LOG.error("Error when transitioning to Active mode", e);
      throw new YarnRuntimeException(e);
    }
  }

  private synchronized void transitionToStandby(boolean initialize)
      throws Exception {
    if (haState == HAServiceState.STANDBY) {
      LOG.info("Already in standby state");
      return;
    }

    LOG.info("Transitioning to standby");
    if (haState == HAServiceState.ACTIVE) {
      rm.stopActiveServices();
      if (initialize) {
        rm.createAndInitActiveServices();
      }
    }
    haState = HAServiceState.STANDBY;
    LOG.info("Transitioned to standby");
  }

  @Override
  public synchronized void transitionToStandby(StateChangeRequestInfo reqInfo)
      throws AccessControlException {
    checkAccess("transitionToStandby");
    checkTransitionAllowed(reqInfo);
    try {
      transitionToStandby(true);
    } catch (Exception e) {
      LOG.error("Error when transitioning to Standby mode", e);
      throw new YarnRuntimeException(e);
    }
  }

  @Override
  public synchronized HAServiceStatus getServiceStatus() throws IOException {
    checkAccess("getServiceStatus");
    HAServiceStatus ret = new HAServiceStatus(haState);
    if (haState == HAServiceState.ACTIVE || haState == HAServiceState.STANDBY) {
      ret.setReadyToBecomeActive();
    } else {
      ret.setNotReadyToBecomeActive("State is " + haState);
    }
    return ret;
  }

  private void checkAccess(String method) throws AccessControlException {
    try {
      RMServerUtils.verifyAccess(adminAcl, method, LOG);
    } catch (YarnException e) {
      throw new AccessControlException(e);
    }
  }

  /**
   * Check that a request to change this RM's HA state is valid.
   * In particular, verifies that, if auto failover is enabled, non-forced
   * requests from the RMHAAdminCLI are rejected, and vice versa.
   *
   * @param req the request to check
   * @throws org.apache.hadoop.security.AccessControlException if the request is
   * disallowed
   */
  private void checkTransitionAllowed(StateChangeRequestInfo req)
      throws AccessControlException {
    if (req.getSource() == RequestSource.REQUEST_BY_USER &&
        autoFailoverEnabled) {
      throw new AccessControlException("Manual HA control for this " +
          "ResourceManager is disallowed, because automatic HA is enabled.");
    } else if (req.getSource() == RequestSource.REQUEST_BY_USER_FORCED &&
        autoFailoverEnabled) {
      LOG.warn("Allowing manual HA control from " + Server.getRemoteAddress() +
          " even though automatic HA is enabled, because the user " +
          "specified the force flag");
    } else if (req.getSource() == RequestSource.REQUEST_BY_ZKFC &&
        !autoFailoverEnabled) {
      throw new AccessControlException("Request from ZK failover controller " +
          "at " + Server.getRemoteAddress() + " denied since automatic HA " +
          "is not enabled");
    }
  }
}
