package com.jointsky.test;

import com.jointsky.bean.RealTimeData;
import com.jointsky.dao.RealTimeDataDao;
import com.jointsky.util.CassandraUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by LiuZifan on 2017/6/29.
 */
public class TestCassandraUtil {
    public static void main(String[] args) {
/*        CassandraUtil cassandraUtil = new CassandraUtil();
        cassandraUtil.connect();*/

        String message = "0152316J222222,0,B02,6f1c4d03-5fe2-4eb0-bf97-2b7f5c55e9f2,20170616235038,,,270.941,,,,133.963,,N";
        RealTimeData realTimeData = new RealTimeData(message);

        RealTimeDataDao realTimeDataDao = new RealTimeDataDao();
        try {
            realTimeDataDao.insert(realTimeData);
        } catch (Exception e) {
            e.printStackTrace();
        }

/*        realTimeData.setMn("aaaasssssdddd");
        realTimeData.setFlagV("0");
        realTimeData.setPollutantCode("B02");
        realTimeData.setPackageID("asdasdasda");
        realTimeData.setDataTime("20170501234");
        realTimeData.setSampleTime("");
        realTimeData.setMin(null);
        params.add(103.2f);
        params.add(null);
        params.add(null);
        params.add(null);
        params.add(134.10f);
        params.add(null);
        params.add("N");*/



    }
}
