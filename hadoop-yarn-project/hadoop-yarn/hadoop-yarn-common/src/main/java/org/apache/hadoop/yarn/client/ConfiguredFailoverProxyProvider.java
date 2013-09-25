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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;

@InterfaceAudience.Private
public abstract class ConfiguredFailoverProxyProvider<T> implements
    FailoverProxyProvider <T> {
  private static final Log LOG = LogFactory.getLog
      (ConfiguredFailoverProxyProvider.class);

  private int currentProxyIndex = 0;
  private final String[] rmServiceIds;
  protected final Class<T> protocol;
  protected final YarnConfiguration conf;

  public ConfiguredFailoverProxyProvider(Configuration configuration,
                                         Class<T> protocol) {
    checkAllowedProtocols(protocol);
    this.conf = new YarnConfiguration(configuration);
    this.protocol = protocol;
    this.rmServiceIds = HAUtil.getRMHAIds(conf).toArray(new String[2]);
  }

  public abstract void checkAllowedProtocols(Class<T> protocol);

  public abstract InetSocketAddress getRMAddress() throws IOException;

  @Override
  public T getProxy() {
    UserGroupInformation ugi;
    final InetSocketAddress rmAddress;
    try {
      ugi = UserGroupInformation.getCurrentUser();
      rmAddress = getRMAddress();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Connecting to RM at " + rmAddress);
      }
    } catch (IOException ioe) { return null;}

    if (ugi == null) {
      return null;
    }

    return ugi.doAs(
        new PrivilegedAction<T>() {
          @Override
          public T run() {
            return (T) YarnRPC.create(conf).getProxy(
                protocol, rmAddress, conf);
          }
        });
  }


  @Override
  public void performFailover(T currentProxy) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Performing failover " + currentProxyIndex);
    }
    currentProxyIndex = (currentProxyIndex + 1) % rmServiceIds.length;
    conf.set(YarnConfiguration.RM_HA_ID, rmServiceIds[currentProxyIndex]);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Performed failover to RM " + rmServiceIds[currentProxyIndex]);
    }
  }

  @Override
  public Class<T> getInterface() {
    return protocol;
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }
}
