package com.jointsky.dao;

import com.jointsky.bean.RealTimeData;
import com.jointsky.util.CassandraUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Cassandra对 RealTimeData进行操作
 * Created by LiuZifan on 2017/6/26.
 */
public class RealTimeDataDao {

    private CassandraUtil cassandraUtil = new CassandraUtil();

    public void insert(RealTimeData realTimeData) throws Exception{

        List<Object> params = new ArrayList<>();
        params.add(realTimeData.getMn());
        params.add(realTimeData.getFlagV());
        params.add(realTimeData.getPollutantCode());
        params.add(realTimeData.getPackageID());
        params.add(realTimeData.getDataTime());
        params.add(realTimeData.getSampleTime());
        params.add(realTimeData.getMin());
        params.add(realTimeData.getRtd());
        params.add(realTimeData.getMax());
        params.add(realTimeData.getCou());
        params.add(realTimeData.getZsMin());
        params.add(realTimeData.getZsRtd());
        params.add(realTimeData.getZsMax());
        params.add(realTimeData.getFlag());

        String sql = "insert into zxjc.realtimedata(MN,FlagV,PollutantCode,PackageID,DataTime,SampleTime,Min,Rtd,Max,Cou,ZsMin,ZsRtd,ZsMax,Flag) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        cassandraUtil.connect();
        cassandraUtil.updateData(sql,params);
    }

}
