/*
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

package co.decodable;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

/** Args: --kafka.brokers localhost:9092 --kafka.in.topic datasrc --kafka.out.topic datasink */
public class PassthroughKafka {
  public static void main(String[] args) throws Exception {
    var parameters = ParameterTool.fromArgs(args);
    var env = initMiniClusterWithEnv();

    var passthroughJob = getPassthroughJob(env, parameters);
    env.execute(passthroughJob);
  }

  private static StreamExecutionEnvironment initMiniClusterWithEnv() throws Exception {
    var flinkConfig = new Configuration();
    flinkConfig.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("8m"));
    flinkConfig.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse("8m"));

    MiniClusterConfiguration clusterConfig =
        new MiniClusterConfiguration.Builder()
            .setNumTaskManagers(1)
            .setNumSlotsPerTaskManager(1)
            .setConfiguration(flinkConfig)
            .build();
    var cluster = new MiniCluster(clusterConfig);
    cluster.start();
    return new RemoteStreamEnvironment(
        cluster.getRestAddress().get().getHost(),
        cluster.getRestAddress().get().getPort(),
        cluster.getConfiguration());
  }

  private static StreamGraph getPassthroughJob(
      StreamExecutionEnvironment env, ParameterTool parameters) {

    var brokers = parameters.getRequired("kafka.brokers");
    KafkaSource<String> source =
        KafkaSource.<String>builder()
            .setBootstrapServers(brokers)
            .setTopics(parameters.getRequired("kafka.in.topic"))
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

    var data = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
    var filtered = data.filter(new DropOnePercent());

    KafkaSink<String> sink =
        KafkaSink.<String>builder()
            .setBootstrapServers(brokers)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(parameters.getRequired("kafka.out.topic"))
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

    filtered.sinkTo(sink);
    return env.getStreamGraph();
  }

  private static class DropOnePercent implements FilterFunction<String> {
    private transient int cnt = 0;

    @Override
    public boolean filter(String value) {
      if (cnt++ == 99) {
        cnt = 0;
        return false;
      }
      return true;
    }
  }
}
