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
package org.apache.heron.integration_test.topology.end_to_end_effectively_once;

import java.io.IOException;
import java.net.MalformedURLException;
import java.text.ParseException;
import java.util.Map;

import org.apache.heron.api.Config;
import org.apache.heron.api.bolt.BaseRichBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.ITwoPhaseStatefulComponent;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.integration_test.common.AbstractTestTopology;
import org.apache.heron.integration_test.core.HttpUtils;
import org.apache.heron.integration_test.core.TestTopologyBuilder;

public class StatefulWithSideEffects extends AbstractTestTopology {

  StatefulWithSideEffects(String[] args) throws MalformedURLException {
    super(args);
  }

  @Override
  protected TestTopologyBuilder buildTopology(TestTopologyBuilder builder) {
    String flushLocation = String.format(
        "%s/incremental/flush/%s", this.httpServerResultsBaseUrl, this.topologyName);
    String writeLocation = String.format(
        "%s/incremental/write/%s", this.httpServerResultsBaseUrl, this.topologyName);

    builder.setSpout("2pc-stateful-spout", new MySpout(), 1, 1000);
    builder.setBolt("2pc-stateful-bolt", new MyBolt(writeLocation), 1)
        .shuffleGrouping("2pc-stateful-spout");

    builder.setTerminalBoltClass(
        "org.apache.heron.integration_test.core.PartialResultAggregatorBolt");
    builder.setOutputLocation(flushLocation);

    return builder;
  }

  public static void main(String[] args) throws Exception {
    Config conf = new Config();
    conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.EFFECTIVELY_ONCE);
    conf.setTopologyStatefulCheckpointIntervalSecs(1);

    StatefulWithSideEffects topology = new StatefulWithSideEffects(args);
    topology.submit(conf);
  }

  static class MySpout extends BaseRichSpout implements ITwoPhaseStatefulComponent<String, String> {
    @Override
    public void open(
        Map<String, Object> conf,
        TopologyContext context,
        SpoutOutputCollector collector) {
    }

    @Override
    public void nextTuple() {
    }

    @Override
    public void postSave(String checkpointId) {
    }

    @Override
    public void preRestore(String checkpointId) {
    }

    @Override
    public void initState(State<String, String> state) {
    }

    @Override
    public void preSave(String checkpointId) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
  }

  static class MyBolt extends BaseRichBolt implements ITwoPhaseStatefulComponent<String, String> {

    private final String resultWriteLocation;

    MyBolt(String resultWriteLocation) {
      this.resultWriteLocation = resultWriteLocation;
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
    public void postSave(String checkpointId) {
      try {
        HttpUtils.httpJsonPost(resultWriteLocation, "commit");
      } catch (IOException | ParseException ignored) {
      }
    }

    @Override
    public void preRestore(String checkpointId) {
      try {
        HttpUtils.httpJsonPost(resultWriteLocation, "abort");
      } catch (IOException | ParseException ignored) {
      }
    }

    @Override
    public void initState(State<String, String> state) {
      try {
        HttpUtils.httpJsonPost(resultWriteLocation, "begin");
      } catch (IOException | ParseException ignored) {
      }
    }

    @Override
    public void preSave(String checkpointId) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
  }

}
