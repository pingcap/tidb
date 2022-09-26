import com.pingcap.kafkareader.proto.BinLogInfo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @description: kafka消费binlog java demo
 * @author: ceaserwang@outlook.com
 * @create: 2019-08-15 21:45
 **/
public class Booter {
    /**
     * 主题
     */
    private  static  String topic =  "6717826900501472462_obinlog";
    /**
     * kafka brokers
     */
    private  static  String serever =  "192.168.138.22:9092,192.168.138.23:9092,192.168.138.24:9092";

    /**
     * 消费者偏移量
     */
    private static long offset = 60;

    /**
     * 消费者线程
     */
    private Thread kafkaConsumerThread;
    /**
     * 消费者
     */
    private KafkaConsumer<String, byte []> consumer;

    public static void main(String[] args) {
        Booter booter = new Booter();
        booter.init();
    }

    public void init() {
        Properties props = assembleConsumerProperties();
        this.consumer = new KafkaConsumer(props);
        consumer.assign(Arrays.asList(new TopicPartition(Booter.topic,0)));
        kafkaConsumerThread = new Thread(() -> {
            Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
            while (true) {
                try {
                    // 指定分区消费的某个offset消费
                    consumer.seek(new TopicPartition(Booter.topic, 0), offset);
                    ConsumerRecords<String, byte []> records = consumer.poll(200);
                    for (ConsumerRecord<String, byte []> record : records) {
                        try {
                            //处理消息
                            dealMessage(record.value());
                            currentOffsets.put(new TopicPartition(Booter.topic, record.partition()), new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                            //提交
                            consumer.commitSync(currentOffsets);
                            currentOffsets.clear();
                            //记录消息offset到db
                        } catch (Exception ie) {
                            //当前消息处理失败
                            currentOffsets.clear();
                        }
                    }
                } catch (Exception e) {
                    currentOffsets.clear();
                }
            }
        });
        kafkaConsumerThread.setName("kafkaConsumerThread");
        kafkaConsumerThread.start();
    }

    private void dealMessage(byte[] value) throws Exception {
        BinLogInfo.Binlog binlog = BinLogInfo.Binlog.parseFrom(value);
        System.out.println(binlog.toString());
    }

    private Properties assembleConsumerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers",Booter.serever);
        props.put("group.id", "mytest");
        //自动提交位移关闭
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("max.poll.records", "10");
        //必须使用ByteArrayDeserializer
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return props;
    }

}
