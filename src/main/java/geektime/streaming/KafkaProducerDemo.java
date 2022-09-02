package geektime.streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

public class KafkaProducerDemo {

    public static void main(String[] args) throws Exception{



        Properties kafkaPropertie = new Properties();
        //配置broker地址，配置多个容错
        kafkaPropertie.put("bootstrap.servers", "localhost:9092");
        //配置key-value允许使用参数化类型
        kafkaPropertie.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaPropertie.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        // 开启 GZIP 压缩
        KafkaProducer kafkaProducer = new KafkaProducer(kafkaPropertie);



        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Map data = new HashMap<>();
        data.put("timestamp", new Date());
        data.put("name","alex");
        data.put("name","alex");



        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("spark_topic_1", kafkaPropertie.toString());

        long time1 = System.currentTimeMillis();
        // for(int i =0 ; i<bathSize ;i++){poll
        kafkaProducer.send(record);


//         Thread.sleep(sleepTime);
    }
}
