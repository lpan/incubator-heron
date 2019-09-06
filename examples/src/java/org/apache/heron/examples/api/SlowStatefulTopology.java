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


package org.apache.heron.examples.api;

import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;

import org.apache.heron.api.Config;
import org.apache.heron.api.HeronSubmitter;
import org.apache.heron.api.bolt.BaseRichBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.exception.AlreadyAliveException;
import org.apache.heron.api.exception.InvalidTopologyException;
import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.ITwoPhaseStatefulComponent;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.common.basics.ByteAmount;

public final class SlowStatefulTopology {

  private SlowStatefulTopology() {
  }

  public static class WordSpout extends BaseRichSpout
      implements ITwoPhaseStatefulComponent<String, Integer> {
    private static final Logger logger = Logger.getLogger(WordSpout.class.getName());

    private static final int ARRAY_LENGTH = 8;

    private String[] words;

    private SpoutOutputCollector collector;

    private State<String, Integer> myState;

    @Override
    public void initState(State<String, Integer> state) {
      this.myState = state;
      if (!state.containsKey("_")) {
        state.put("_", 0);
      }
    }

    @Override
    public void preSave(String checkpointId) {
      // Nothing really since we operate out of the system supplied state
    }

    @Override
    public void preRestore(String checkpointId) {
      logger.info("Failure detected, topology is being restored to checkpoint ID: " + checkpointId);
    }

    @Override
    public void postSave(String checkpointId) {
      logger.info("Checkpoint ID: " + checkpointId + " has become globally consistent");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
      outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {

      words = new String[] {
        "1", "2", "3", "4", "5", "6", "7", "8"
      };
      collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
      try {
        Thread.sleep(500);
      } catch (Exception e) {
        //
      }

      int nextInt = myState.get("_");
      String nextWord = (words[nextInt]);

      nextInt ++;
      if (nextInt == ARRAY_LENGTH) {
        nextInt = 0;
      }

      myState.put("_", nextInt);

      logger.info(nextWord);
      collector.emit(new Values(nextWord));
    }
  }

  /**
   * A bolt that counts the words that it receives
   */
  public static class ConsumerBolt extends BaseRichBolt
      implements ITwoPhaseStatefulComponent<String, Integer> {

    private static final Logger logger = Logger.getLogger(ConsumerBolt.class.getName());

    private static final long serialVersionUID = -5470591933906954522L;

    private OutputCollector collector;
    private Map<String, Integer> countMap;
    private State<String, Integer> myState;

    @Override
    public void initState(State<String, Integer> state) {
      this.myState = state;
    }

    @Override
    public void preSave(String checkpointId) {
      logger.info("preSave: " + checkpointId);
    }

    @Override
    public void preRestore(String checkpointId) {
    }

    @Override
    public void postSave(String checkpointId) {
      logger.info("postSave: " + checkpointId);
    }

    @SuppressWarnings("rawtypes")
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
      collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
      String word = tuple.getString(0);
      logger.info(word);

      try {
        PrintWriter writer = new PrintWriter(new FileWriter("myfile.txt", true));
        writer.println(word);
        writer.close();
      } catch (IOException e) {
        // noop
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
  }

  /**
   * Main method
   */
  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
    if (args.length < 1) {
      throw new RuntimeException("Specify topology name");
    }

    int parallelism = 1;
    if (args.length > 1) {
      parallelism = Integer.parseInt(args[1]);
    }
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("word", new WordSpout(), parallelism);
    builder.setBolt("consumer", new ConsumerBolt(), parallelism)
        .fieldsGrouping("word", new Fields("word"));
    Config conf = new Config();
    conf.setNumStmgrs(parallelism);
    conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.EFFECTIVELY_ONCE);
    conf.setTopologyStatefulCheckpointIntervalSecs(20);

    // configure component resources
    conf.setComponentRam("word",
        ByteAmount.fromMegabytes(ExampleResources.COMPONENT_RAM_MB));
    conf.setComponentRam("consumer",
        ByteAmount.fromMegabytes(ExampleResources.COMPONENT_RAM_MB));

    // configure container resources
    conf.setContainerDiskRequested(
        ExampleResources.getContainerDisk(2 * parallelism, parallelism));
    conf.setContainerRamRequested(
        ExampleResources.getContainerRam(4 * parallelism, parallelism));
    conf.setContainerCpuRequested(
        ExampleResources.getContainerCpu(2 * parallelism, parallelism));

    HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }
}
