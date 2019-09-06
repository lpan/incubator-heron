/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.integration_test.core;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;

public class PartialResultAggregatorBolt extends BaseBatchBolt {
  private final String httpPostUrl;
  private static final Logger LOG = Logger.getLogger(PartialResultAggregatorBolt.class.getName());

  public PartialResultAggregatorBolt(String outputLocation) {
    this.httpPostUrl = outputLocation;
  }

  @Override
  public void prepare(
      Map<String, Object> heronConf,
      TopologyContext context,
      OutputCollector collector) {
  }

  @Override
  public void execute(Tuple input) {
  }

  @Override
  public void finishBatch() {
    LOG.info(String.format("Posting %s to flush actual result",  httpPostUrl));
    try {
      int responseCode = -1;
      for (int attempts = 0; attempts < 2; attempts++) {
        responseCode = HttpUtils.httpJsonPost(httpPostUrl, "");
        if (responseCode == 200) {
          return;
        }
      }
      throw new RuntimeException(
          String.format("Failed to post to %s: %s",  httpPostUrl, responseCode));
    } catch (IOException | ParseException e) {
      throw new RuntimeException(String.format("Posting to %s failed",  httpPostUrl), e);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}
