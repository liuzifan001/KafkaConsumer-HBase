package com.jointsky.bean;

import java.sql.Date;

/**实时数据(水、气数据结构一致)
 * Created by LiuZifan on 2017/6/25.
 */
public class RealTimeData {
    private String mn;                   //设备唯一编码
    private String flagV;
    private String packageID;
    private String pollutantCode;       //污染物编码
    private String dataTime;             //时间
    private String sampleTime;
    private float min;
    private float rtd;
    private float max;
    private float cou;
    private float zsMin;
    private float zsRtd;
    private float zsMax;
    private String flag;

    public RealTimeData() {

    }

    /**
     * 将以逗号分隔的kafka消息转化为bean
     * @param kafkaMessage
     */
    public RealTimeData(String kafkaMessage) {
        String[] values = kafkaMessage.split(",");
        if(values.length == 14){
            mn = values[0];
            flagV = values[1];
            pollutantCode = values[2];
            packageID = values[3];
            dataTime = values[4];
            sampleTime = values[5];
            min = values[6].isEmpty()? 0.0f:Float.parseFloat(values[6]);
            rtd = values[7].isEmpty()? 0.0f:Float.parseFloat(values[7]);
            max = values[8].isEmpty()? 0.0f:Float.parseFloat(values[8]);
            cou = values[9].isEmpty()? 0.0f:Float.parseFloat(values[9]);
            zsMin = values[10].isEmpty()? 0.0f:Float.parseFloat(values[10]);
            zsRtd = values[11].isEmpty()? 0.0f:Float.parseFloat(values[11]);
            zsMax = values[12].isEmpty()? 0.0f:Float.parseFloat(values[12]);
            flag = values[13];
        }
    }



    /**
     * 将String型的date进行转化（以后再写）
     * @param date
     * @return
     */
    public Date getFormatDate(String date) {
        return null;
    }


    //getter and setter

    public String getMn() {
        return mn;
    }

    public void setMn(String mn) {
        this.mn = mn;
    }

    public String getFlagV() {
        return flagV;
    }

    public void setFlagV(String flagV) {
        this.flagV = flagV;
    }

    public String getPollutantCode() {
        return pollutantCode;
    }

    public void setPollutantCode(String pollutantCode) {
        this.pollutantCode = pollutantCode;
    }

    public String getPackageID() {
        return packageID;
    }

    public void setPackageID(String packageID) {
        this.packageID = packageID;
    }

    public String getDataTime() {
        return dataTime;
    }

    public void setDataTime(String dataTime) {
        this.dataTime = dataTime;
    }

    public String getSampleTime() {
        return sampleTime;
    }

    public void setSampleTime(String sampleTime) {
        this.sampleTime = sampleTime;
    }

    public float getMin() {
        return min;
    }

    public void setMin(float min) {
        this.min = min;
    }

    public float getRtd() {
        return rtd;
    }

    public void setRtd(float rtd) {
        this.rtd = rtd;
    }

    public float getMax() {
        return max;
    }

    public void setMax(float max) {
        this.max = max;
    }

    public float getCou() {
        return cou;
    }

    public void setCou(float cou) {
        this.cou = cou;
    }

    public float getZsMin() {
        return zsMin;
    }

    public void setZsMin(float zsMin) {
        this.zsMin = zsMin;
    }

    public float getZsRtd() {
        return zsRtd;
    }

    public void setZsRtd(float zsRtd) {
        this.zsRtd = zsRtd;
    }

    public float getZsMax() {
        return zsMax;
    }

    public void setZsMax(float zsMax) {
        this.zsMax = zsMax;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }



    /**
     * 生成HBase 的RowKey(先留着吧)
     * @return RowKey
     */
    public String getRowKey() {
        return "" + dataTime + mn + pollutantCode;
    }
}
