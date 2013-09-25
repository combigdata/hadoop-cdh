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

package org.apache.hadoop.yarn.server.api;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.client.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.yarn.client.RMProxy;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class ServerRMProxy<T> extends RMProxy<T> {
  private static final Log LOG = LogFactory.getLog(ServerRMProxy.class);

  @InterfaceAudience.Private
  static class ServerProxyProvider<T>
      extends ConfiguredFailoverProxyProvider<T> {

    ServerProxyProvider(Configuration conf, Class<T> xface) {
      super(conf, xface);
    }

    @Override
    public void checkAllowedProtocols(Class<T> protocol) {
      Preconditions.checkArgument(
          protocol.isAssignableFrom(ResourceTracker.class),
          "RM does not support this protocol");
    }

    @Override
    public InetSocketAddress getRMAddress() throws IOException {
      return ServerRMProxy.getRMAddress(conf, protocol);
    }
  }

  public static <T> T createRMProxy(final Configuration configuration,
      final Class<T> protocol) throws IOException {
    YarnConfiguration conf = (configuration instanceof YarnConfiguration)
        ? (YarnConfiguration) configuration
        : new YarnConfiguration(configuration);
    if (HAUtil.isHAEnabled(conf)) {
      ServerProxyProvider<T> provider =
          new ServerProxyProvider<T>(conf, protocol);
      return createRMProxy(conf, protocol, provider);
    } else {
      InetSocketAddress rmAddress = getRMAddress(conf, protocol);
      return createRMProxy(conf, protocol, rmAddress);
    }
  }

  private static InetSocketAddress getRMAddress(YarnConfiguration conf,
                                                Class<?> protocol) {
    if (protocol == ResourceTracker.class) {
      return conf.getSocketAddr(
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT);
    } else {
      String message = "Unsupported protocol found when creating the proxy " +
          "connection to ResourceManager: " +
          ((protocol != null) ? protocol.getClass().getName() : "null");
      LOG.error(message);
      throw new IllegalStateException(message);
    }
  }
}