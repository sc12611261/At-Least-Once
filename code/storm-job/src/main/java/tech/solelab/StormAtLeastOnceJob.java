package tech.solelab;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;

public class StormAtLeastOnceJob {

    // 1. 解析 Bolt
    public static class ParserBolt extends BaseRichBolt {
        OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            try {
                String json = input.getStringByField("value");
                JSONObject msg = JSONObject.parseObject(json);

    
                // 第一个参数必须是 input，告诉 Storm 新数据是 input 的孩子
                this.collector.emit(input, new Values(
                        msg.getInteger("msg_id"),
                        msg.getString("timestamp"),
                        msg.getJSONObject("content")
                ));

                this.collector.ack(input);
            } catch (Exception e) {
                e.printStackTrace();
                this.collector.fail(input);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("msg_id", "timestamp", "content"));
        }
    }

    // 2. 过滤 Bolt
    public static class FilterBolt extends BaseRichBolt {
        OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            try {
                Integer msgId = input.getIntegerByField("msg_id");

                if (msgId != null && msgId % 2 == 0) {
                    System.out.println("FilterBolt: emitting msg_id=" + msgId);
                    // 锚定 (Anchoring)！
                    this.collector.emit(input, new Values(
                            msgId,
                            input.getStringByField("timestamp"),
                            (JSONObject) input.getValueByField("content")
                    ));
                } else {
                    // 即使被过滤掉，也要 ack，否则 Spout 会超时重发
                    // System.out.println("Filtered out: " + msgId);
                }

                this.collector.ack(input);
            } catch (Exception e) {
                this.collector.fail(input);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("msg_id", "timestamp", "content"));
        }
    }

    // 3. 窗口计数 Bolt
    public static class WindowCountBolt extends BaseWindowedBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(TupleWindow window) {
            Map<Integer, Integer> counts = new HashMap<>();
            Map<Integer, String> lastTimestamp = new HashMap<>();

            for (Tuple tuple : window.get()) {
                Integer msgId = tuple.getIntegerByField("msg_id");
                String timestamp = tuple.getStringByField("timestamp");
                if (msgId != null && timestamp != null) {
                    counts.put(msgId, counts.getOrDefault(msgId, 0) + 1);
                    lastTimestamp.put(msgId, timestamp);
                }
            }

            for (Integer msgId : counts.keySet()) {
                Map<String, Object> result = new HashMap<>();
                result.put("original_msg_id", msgId);
                result.put("origin_timestamp", lastTimestamp.get(msgId));
                result.put("window_time", String.valueOf(System.currentTimeMillis()));
                result.put("sensor_id", "");
                result.put("count_result", counts.get(msgId));

                String key = String.valueOf(msgId);

                
                // 我们把 window.get() 里的所有 tuple 都作为锚点
                // 这样如果 Kafka Sink 写失败，整个窗口会被重算
                collector.emit(window.get(), new Values(key, JSONObject.toJSONString(result)));
            }

            // BaseWindowedBolt 会自动 ack 窗口内的 tuple，不需要手动 ack
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("key", "message"));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: storm jar ... <topology-name>");
            System.exit(1);
        }

        String bootstrapServers = "49.52.27.49:9092";
        String sourceTopic = "storm-test-topic";
        String sinkTopic = "storm-result-topic";

        TopologyBuilder builder = new TopologyBuilder();

        // 1. 配置 Kafka Spout (明确开启 At Least Once)
        KafkaSpoutConfig<String, String> spoutConfig = KafkaSpoutConfig.builder(bootstrapServers, sourceTopic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "storm-source-group")
                .setProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                .build();
        builder.setSpout("kafka-spout", new KafkaSpout<>(spoutConfig));

        builder.setBolt("parser-bolt", new ParserBolt()).shuffleGrouping("kafka-spout");
        builder.setBolt("filter-bolt", new FilterBolt()).shuffleGrouping("parser-bolt");

        builder.setBolt("window-bolt", new WindowCountBolt()
                        .withTumblingWindow(new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS)))
                .fieldsGrouping("filter-bolt", new Fields("msg_id"));

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "1"); // 这里的 ack 是 Kafka Producer 的 ack，不是 Storm 的
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaBolt<String, String> kafkaBolt = new KafkaBolt<String, String>()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector(sinkTopic))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("key", "message"));

        builder.setBolt("kafka-sink", kafkaBolt).shuffleGrouping("window-bolt");

        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.setMessageTimeoutSecs(60);
      
        conf.setNumAckers(1);

        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
}
