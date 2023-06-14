package co.decodable.util.datagen;

import org.apache.commons.text.CharacterPredicates;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.XORShiftRandom;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class SpeedValidator {
  public static void main(String[] args) throws Exception {
    ParameterTool pt = ParameterTool.fromArgs(args);
    int recordSize = pt.getInt("recordSize", 200);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(pt);
    env.setParallelism(1);

    // launch job to validate input throughput
    KafkaSource<String> source =
        KafkaSource.<String>builder()
            .setBootstrapServers(pt.getRequired("kafka.brokers"))
            .setTopics(pt.getRequired("kafka.in.topic"))
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
    env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka")
        .setParallelism(2)
        .flatMap(new Throughput<>("Consumer:", recordSize, pt.getLong("logfreq", 200_000L)))
        .setParallelism(1);
    env.execute("TP logger");
  }
}
