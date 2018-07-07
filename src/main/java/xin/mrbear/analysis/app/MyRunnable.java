package xin.mrbear.analysis.app;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import redis.clients.jedis.Jedis;
import xin.mrbear.analysis.storm.dao.LogAnalyzerDao;
import xin.mrbear.analysis.storm.domain.BaseRecord;
import xin.mrbear.analysis.storm.domain.LogAnalyzeJob;
import xin.mrbear.analysis.storm.util.RedisPool;

public class MyRunnable implements Runnable {
    private LogAnalyzerDao logAnalyzerDao = new LogAnalyzerDao();
    private HashMap<String, Integer> pvCacheMap = new HashMap<String, Integer>();
    private HashMap<String, Integer> uvCacheMap = new HashMap<String, Integer>();

    public void run() {
        //这个run方法会被一分钟执行一次
        // 获取这一分钟的值  减去上一分钟的
        // 1.获取这一分钟的值
        List<LogAnalyzeJob> logAnalyzeJobs = logAnalyzerDao.loadJobList();
        ArrayList<BaseRecord> baseRecords = new ArrayList<BaseRecord>();
        for (LogAnalyzeJob logAnalyzeJob : logAnalyzeJobs) {
            String jobId = logAnalyzeJob.getJobId();
            String pvKey = "loganalyzer:" + jobId + ":pv:20190808";
            Jedis jedis = RedisPool.getJedis();
            String value = jedis.get(pvKey);
            //2.获取上一分钟
            Integer oldValue = pvCacheMap.get(pvKey);
            if(oldValue == null){
                oldValue = 0;
            }
            int appendValue = Integer.parseInt(value) - oldValue;
            pvCacheMap.put(pvKey,Integer.parseInt(value));


            //计算UV的增量数据
            String uvKey = "loganalyzer:" + jobId + ":uv:20190808";
            int uvValue = jedis.scard(uvKey).intValue();

            //2.获取上一分钟的值
            Integer oldUvValue = uvCacheMap.get(uvKey);
            //3.减法
            if (oldUvValue == null){
                oldValue = 0;
            }
            int appendUvValue = uvValue - oldUvValue;
            uvCacheMap.put(uvKey,uvValue);

            baseRecords.add(new BaseRecord(jobId,appendValue,appendUvValue,new Date()));

        }


        //保存到数据库
        System.out.println("----------保存到数据库"+baseRecords);
        logAnalyzerDao.saveMinuteAppendRecord(baseRecords);

    }
}
