package com.jointsky.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**实现对HBase的CURD操作
 * Created by LiuZifan on 2017/6/23.
 */
public class HBaseUtil {
    private static Configuration conf = null;
    private static final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);    //日志
    private static PropertiesLoader loader = new PropertiesLoader("kafka.properties");
    private static Connection conn = null;
    private static Admin admin = null;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",loader.getProperty("hbase.zookeeper.quorum"));

        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     * @param tableName 表名
     * @param family 列族列表
     * @param isdelete 表存在时是否删除
     * @throws Exception
     */
    public static void createTable(String tableName, String[] family,boolean isdelete) throws Exception {

        TableName tn = TableName.valueOf(tableName);
        try {
            if (admin.tableExists(tn)){
                //HBase的表必须先disable才能删除
                admin.disableTable(tn);
                admin.deleteTable(tn);
                System.out.println("表名为：" + tableName + "已存在，旧表已删除");
            }
            else {
                throw new Exception("建表isDelete为flase，同名表:"+tableName+"已存在");
            }
            //表描述对象
            HTableDescriptor htd = new HTableDescriptor(tn);

            for (String cf: family) {
                htd.addFamily(new HColumnDescriptor(cf));
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            closeConnect(null,null,admin,null,null);
        }


    }

    /**
     * 异步批量写入数据
     * @param tableName
     * @param puts
     * @throws Exception
     */
   public static void addDataAsync(String tableName, List<Put> puts) throws Exception {
       //创建监听器
       final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
           @Override
           public void onException(RetriesExhaustedWithDetailsException e,
                                   BufferedMutator mutator) {
               for (int i = 0; i < e.getNumExceptions(); i++) {
                   logger.error("写入失败： " + e.getRow(i) + "！");
               }
           }
       };

       //设置表的缓存参数
       BufferedMutatorParams params = new BufferedMutatorParams(
               TableName.valueOf(tableName)).listener(listener);
       params.writeBufferSize(5 * 1024 * 1024);      //写缓存为5G

       // 操作类获取
       final BufferedMutator mutator = conn.getBufferedMutator(params);

       try{
           mutator.mutate(puts);
           mutator.flush();
       }finally {
           closeConnect(null,mutator,null,null,null);
       }
   }

    /**
     * 同步批量添加数据
     * @param tableName
     * @param puts
     * @throws Exception
     */
    public static void addDataSync(String tableName, List<Put> puts) throws Exception{
        HTable hTable = (HTable) conn.getTable(TableName.valueOf(tableName));
        try {
            hTable.put(puts);
            hTable.flushCommits();
        }finally {
            closeConnect(null,null,null,hTable,null);
        }
    }



    /**
     * 关闭连接
     *
     * @param conn
     * @param mutator
     * @param admin
     * @param htable
     */
    public static void closeConnect(Connection conn, BufferedMutator mutator,
                                    Admin admin, HTable htable, Table table) {
        if (null != conn) {
            try {
                conn.close();
            } catch (Exception e) {
                logger.error("closeConnect failure !", e);
            }
        }

        if (null != mutator) {
            try {
                mutator.close();
            } catch (Exception e) {
                logger.error("closeBufferedMutator failure !", e);
            }
        }

        if (null != admin) {
            try {
                admin.close();
            } catch (Exception e) {
                logger.error("closeAdmin failure !", e);
            }
        }
        if (null != htable) {
            try {
                htable.close();
            } catch (Exception e) {
                logger.error("closeHtable failure !", e);
            }
        }
        if (null != table) {
            try {
                table.close();
            } catch (Exception e) {
                logger.error("closeTable failure !", e);
            }
        }
    }

}
