package com.jointsky.test;

import com.jointsky.util.HBaseUtil;
import com.jointsky.util.PropertiesLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by LiuZifan on 2017/6/25.
 */
public class TestHBaseUtil {
    public static void main(String[] args) {
        Put put1 = new Put("test06251132".getBytes());
        put1.addColumn("data".getBytes(),"Flag".getBytes(),"N".getBytes());
        put1.addColumn("data".getBytes(),"RealTimeData".getBytes(),"289.00".getBytes());

        Put put2 = new Put("test06251138".getBytes());
        put2.addColumn("data".getBytes(),"Flag".getBytes(),"N".getBytes());
        put2.addColumn("data".getBytes(),"RealTimeData".getBytes(),"300.00".getBytes());

        List<Put> putList = new ArrayList<Put>();
        putList.add(put1);
        putList.add(put2);
        try {
            HBaseUtil.addDataAsync("testdata:gas_realtimedata",putList);
            System.out.println("success??");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
