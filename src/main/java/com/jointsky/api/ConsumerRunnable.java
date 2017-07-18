package com.jointsky.api;

import com.jointsky.bean.RealTimeData;
import com.jointsky.dao.RealTimeDataDao;
import com.jointsky.util.PropertiesLoader;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Properties;

/**
 * Created by LiuZifan on 2017/6/20.
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerRunnable implements Runnable {

    private final KafkaConsumer<String, String> consumer;              // 每个线程维护私有的KafkaConsumer实例
    private static PropertiesLoader loader = new PropertiesLoader("kafka.properties");

    /**
     * 创建Consumer
     * @param groupId 消费者组id
     * @param topic 主题
     */
    public ConsumerRunnable(String groupId, String topic) {
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers",loader.getProperty("bootstrap.servers"));
        consumerProps.setProperty("key.deserializer", loader.getProperty("key.deserializer"));
        consumerProps.setProperty("value.deserializer",loader.getProperty("value.deserializer"));
        consumerProps.setProperty("enable.auto.commit",loader.getProperty("enable.auto.commit"));                  //自动提交位移设为false
        consumerProps.setProperty("auto.commit.interval.ms",loader.getProperty("auto.commit.interval.ms"));
        consumerProps.setProperty("group.id",groupId);
        this.consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        RealTimeDataDao realTimeDataDao = new RealTimeDataDao();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(200);   // 使用200ms作为获取超时时间
            for (ConsumerRecord<String, String> record : records) {
                // 这里面写处理消息的逻辑，暂时仅简单地打印消息
                System.out.printf("currentThread = %s partition = %d offset = %d, key = %s, value = %s ",
                        Thread.currentThread().getName(),record.partition(), record.offset(), record.key(), record.value() + "\n");
                RealTimeData realTimeData = new RealTimeData(record.value());
                try {
                    realTimeDataDao.insert(realTimeData);
                } catch (Exception e) {
                    e.printStackTrace();
                }


            }
            consumer.commitSync();    //手动提交(这里设置待定)
            //System.out.println("提交了 " + records.count() + "条已消费消息的offset");
        }
    }
}
