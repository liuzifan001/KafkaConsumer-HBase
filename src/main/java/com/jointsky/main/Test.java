package com.jointsky.main;

import com.jointsky.api.ConsumerRunnable;

/**
 * Created by LiuZifan on 2017/6/20.
 */
public class Test {
    public static void main(String[] args) {
        ConsumerRunnable task = new ConsumerRunnable("test","Test620");
        new Thread(task).start();
    }
}
