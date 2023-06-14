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

public class DataGen {
  public static void main(String[] args) throws Exception {
    ParameterTool pt = ParameterTool.fromArgs(args);
    int recordSize = pt.getInt("recordSize", 200);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(pt);
    env.setParallelism(1);

    DataStream<String> stream =
        env.addSource(new GeneratorSource(pt, recordSize))
            // pt.getInt("parallelism.generator", 1)
            .setParallelism(1);

    switch (pt.get("output", "print")) {
      case "print":
        stream.print().setParallelism(1);
        break;
      case "kafka":
        writeToKafka(pt, stream);
        break;
      case "zero":
        stream.addSink(new DiscardingSink<>());
        break;
    }
    stream
        .flatMap(new Throughput<>("Generator:", recordSize, pt.getLong("logfreq", 20_000L)))
        .setParallelism(1);

    env.execute("JSON Data Generator");

    // launch second job to validate input throughput
    KafkaSource<String> source =
        KafkaSource.<String>builder()
            .setBootstrapServers(pt.getRequired("kafka.brokers"))
            .setTopics(pt.getRequired("kafka.in.topic"))
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
    env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka")
        .setParallelism(2)
        .flatMap(new Throughput<>("Consumer:", recordSize, pt.getLong("logfreq", 20_000L)));
    env.execute("TP logger");
  }

  private static void writeToKafka(ParameterTool parameters, DataStream<String> stream) {
    var brokers = parameters.getRequired("kafka.brokers");

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
    stream.sinkTo(sink);
  }

  private static class GeneratorSource extends RichParallelSourceFunction<String> {
    private final ParameterTool parameters;
    private final int recordSize;
    private volatile boolean running = true;
    private final Random RNG = new XORShiftRandom();

    public GeneratorSource(ParameterTool pt, int recordSize) {
      this.parameters = pt;
      this.recordSize = recordSize;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
      long id = 0; // TODO make parallelizable
      // useful for filtering x% of the data
      int numCategories = parameters.getInt("numCategories", 100);
      // useful for "exploding" the keyspace
      long numSubCategories = parameters.getLong("numSubCategories", 1_000_000L);

      int titleLength = parameters.getInt("titleLength", 5);

      // note: uses 30% of the method's CPU time
      // useful for regex tests
      RandomStringGenerator randomStringGenerator =
          new RandomStringGenerator.Builder()
              .withinRange('0', 'z')
              .usingRandom(RNG::nextInt)
              .filteredBy(CharacterPredicates.LETTERS, CharacterPredicates.DIGITS)
              .build();

      // Flink JSON default:
      // https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/formats/json/#json-timestamp-format-standard
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.s");

      while (running) {
        StringBuilder sb = new StringBuilder(recordSize);
        sb.append("{\"id\":");
        sb.append(id++);
        sb.append(",\"categoryId\":");
        sb.append(RNG.nextInt(numCategories));
        sb.append(",\"subCategoryId\":");
        sb.append(nextLong(numSubCategories));
        sb.append(",\"title\":\"");
        sb.append(randomStringGenerator.generate(titleLength));
        sb.append("\",\"createdAt\":");
        sb.append(System.currentTimeMillis());
        sb.append(",\"createdTS\":\"");
        sb.append(LocalDateTime.now().format(formatter));
        sb.append("\",\"padding\":\"");
        // the numbers might have varying sizes, add padding
        int padding = recordSize - sb.length() - 2; // for the "} at the end of the JSON str
        if (padding < 0) {
          throw new RuntimeException("Increase record size. padding is " + padding);
        }
        sb.append("x".repeat(padding));
        sb.append("\"}");

        ctx.collect(sb.toString());
        /* if (id % 1000 == 0) {
          // 25ms => 6?
          // 10ms => 12??
          // 5ms => 25mb/s
          // 3ms => 36mb/s
          // 2ms ==> 50mb/s
          Thread.sleep(2);
        } */
      }
    }

    @Override
    public void cancel() {
      running = true;
    }

    // src: https://stackoverflow.com/questions/2546078/java-random-long-number-in-0-x-n-range
    private long nextLong(long n) {
      // error checking and 2^x checking removed for simplicity.
      long bits, val;
      do {
        bits = (RNG.nextLong() << 1) >>> 1;
        val = bits % n;
      } while (bits - val + (n - 1) < 0L);
      return val;
    }
  }
}
