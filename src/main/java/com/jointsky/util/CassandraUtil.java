package com.jointsky.util;

import com.datastax.driver.core.*;

import java.util.List;

/** 利用Cassandra的java api 对数据库进行操作
 * Created by LiuZifan on 2017/6/29.
 */
public class CassandraUtil {
    private static PropertiesLoader loader = new PropertiesLoader("Cassandra.properties");
    private static String contactPoints = loader.getProperty("contactPoints");
    private static int port = Integer.parseInt(loader.getProperty("port"));

    private Cluster cluster;
    private Session session;

    /**
     * 连接到指定的Cassandra 节点，该节点在配置文件中配置
     */
    public void connect() {
        if (cluster == null)
        cluster = Cluster.builder().addContactPoint(contactPoints).withPort(port).build();
        if (session == null)
        session = cluster.connect();
        System.out.println("connect success!");
    }

    /**
     * 增、删、改
     * @param sql sql语句
     * @param params 传入参数列表
     */
    public void updateData(String sql, List<Object> params) {
        PreparedStatement ps = session.prepare(sql);
        BoundStatement bound = ps.bind();
        int index = 0;

        if (params != null && !params.isEmpty()) {
            for(Object param: params) {
                bound.set(index++, param,(Class)param.getClass());
            }
        }
        session.execute(bound);

    }




    public void close() {
        session.close();
        cluster.close();
    }


}
