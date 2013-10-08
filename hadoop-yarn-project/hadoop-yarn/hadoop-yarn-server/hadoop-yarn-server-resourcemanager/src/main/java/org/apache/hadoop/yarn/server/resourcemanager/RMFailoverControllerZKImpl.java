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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.io.IOException;

@InterfaceAudience.Private
public class RMFailoverControllerZKImpl implements RMFailoverController {
  private static final Log LOG
      = LogFactory.getLog(RMFailoverControllerZKImpl.class);
  private Configuration conf;
  private RMZKFailoverController zkfc;

  public RMFailoverControllerZKImpl() {
    // do nothing - to be used for ReflectionUtils stuff
  }

  private void formatIfNecessary() throws Exception {
    // Format if not formatted
    String[] formatString = {"-formatZK", "-nonInteractive"};
    zkfc.run(formatString);
  }

  @Override
  public void setConf(Configuration conf) {
    if (!(conf instanceof YarnConfiguration)) {
      throw new YarnRuntimeException("Attempting to start " +
          "RMFailoverController with a Configuration which is not " +
          "YarnConfiguration");
    }

    this.conf = conf;
    try {
      this.zkfc = RMZKFailoverController.create(this.conf);
    } catch (IOException e) {
      throw new YarnRuntimeException("Could not create " +
          "RMZKFailoverController", e);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void run() {
    try {
      formatIfNecessary();
      zkfc.run(new String[0]);
    } catch (Exception e) {
      LOG.error("Error encountered while running ZKFailoverController", e);
      throw new YarnRuntimeException(
          "Error encountered while running ZKFailoverController");
    }
  }
}
