package xin.mrbear.analysis.storm.pojo;

import java.lang.reflect.Field;

public class Message {

    private String refUrl; //来源url
    private String requestUrl;  //当前请求的url
    private String reqTime;// 请求的时间
    private String type; // 日志的类型
    private String time;// 停留时间
    private String ip;// ip
    private String sid;// 会话id
    private String user;//用户账户
    private String os;//操作系统
    private String br;//浏览器
    private String cc;//屏幕尺寸
    private String wz;//用户鼠标点击的位置
    private String clstag;//用户点击的标签

    public Message(String[] fields) {
        this.refUrl = fields[0];
        this.requestUrl = fields[1];
        this.reqTime = fields[2];
        this.type = fields[3];
        this.time = fields[4];
        this.ip = fields[5];
        this.sid = fields[6];
        this.user = fields[7];
        this.clstag = fields[8];
        this.os = fields[9];
        this.br = fields[10];
        this.cc = fields[11];
        this.wz = fields[12];
    }

    public String getClstag() {
        return clstag;
    }

    public void setClstag(String clstag) {
        this.clstag = clstag;
    }

    public String getRefUrl() {
        return refUrl;
    }

    public void setRefUrl(String refUrl) {
        this.refUrl = refUrl;
    }

    public String getRequestUrl() {
        return requestUrl;
    }

    public void setRequestUrl(String requestUrl) {
        this.requestUrl = requestUrl;
    }

    public String getReqTime() {
        return reqTime;
    }

    public void setReqTime(String reqTime) {
        this.reqTime = reqTime;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public String getBr() {
        return br;
    }

    public void setBr(String br) {
        this.br = br;
    }

    public String getCc() {
        return cc;
    }

    public void setCc(String cc) {
        this.cc = cc;
    }

    public String getWz() {
        return wz;
    }

    public void setWz(String wz) {
        this.wz = wz;
    }

    @Override
    public String toString() {
        return "Message{" +
            "refUrl='" + refUrl + '\'' +
            ", requestUrl='" + requestUrl + '\'' +
            ", reqTime='" + reqTime + '\'' +
            ", type='" + type + '\'' +
            ", time='" + time + '\'' +
            ", ip='" + ip + '\'' +
            ", sid='" + sid + '\'' +
            ", user='" + user + '\'' +
            ", os='" + os + '\'' +
            ", br='" + br + '\'' +
            ", cc='" + cc + '\'' +
            ", wz='" + wz + '\'' +
            ", clstag='" + clstag + '\'' +
            '}';
    }

    /**
     * 传入一个字段，根据字段的名称，返回messge对象中对应的值
     * 比如  field=requestUrl 返回message对象中requestUrl的值
     * 通过反射 get方法直接获取
     *
     * @param field
     * @return
     */
    public String getCompareValue(String field) {

        try {
            Class<?> messageclz = Class.forName("xin.mrbear.analysis.storm.pojo.Message");
            Field declaredField = messageclz.getDeclaredField(field);
            declaredField.setAccessible(true);
            String name = declaredField.getName();
            return name;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }


    }
}
