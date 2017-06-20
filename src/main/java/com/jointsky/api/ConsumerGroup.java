package com.jointsky.api;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by LiuZifan on 2017/6/20.
 */
public class ConsumerGroup {
    private List<ConsumerRunnable> consumers;

    /**
     * 定义消费者组，可多线程消费消息(暂时不用，因kafka不支持分区外全局顺序消费)
     * @param consumerNum   消费者组成员个数
     * @param groupId        组id
     * @param topic          主题
     */
    public ConsumerGroup(int consumerNum, String groupId, String topic) {
        consumers = new ArrayList<>(consumerNum);
        for (int i = 0; i < consumerNum; ++i) {
            ConsumerRunnable consumerThread = new ConsumerRunnable(groupId, topic);
            consumers.add(consumerThread);
        }
    }

    public void execute() {
        for (ConsumerRunnable task : consumers) {
            new Thread(task).start();
        }
    }
}
