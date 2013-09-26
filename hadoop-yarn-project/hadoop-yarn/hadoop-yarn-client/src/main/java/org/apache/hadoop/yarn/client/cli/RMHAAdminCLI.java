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

package org.apache.hadoop.yarn.client.cli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAAdmin;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.client.RMHAServiceTarget;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.io.IOException;
import java.util.Collection;

public class RMHAAdminCLI extends HAAdmin {
  RMHAAdminCLI() {
    super();
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf != null) {
      conf = new YarnConfiguration(conf);
    }
    super.setConf(conf);
  }

  @Override
  protected HAServiceTarget resolveTarget(String nodeId) {
    Collection<String> rmServiceIds = HAUtil.getRMHAIds(getConf());
    if (!rmServiceIds.contains(nodeId)) {
      String msg = nodeId + " is not a valid serviceId. It should be one of ";
      for (String id : rmServiceIds) {
        msg += id + " ";
      }
      throw new IllegalArgumentException(msg);
    }
    try {
      return new RMHAServiceTarget((YarnConfiguration)getConf(), nodeId);
    } catch (IllegalArgumentException iae) {
      throw new YarnRuntimeException(
          "Could not connect to one of the RMs, the configuration for it " +
              "might be missing");
    } catch (IOException ioe) {
      throw new YarnRuntimeException(
          "Could not connect to RM HA Admin for node " + nodeId);
    }
  }

  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new RMHAAdminCLI(), argv);
    System.exit(res);
  }
}
