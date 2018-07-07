package xin.mrbear.analysis.storm.util;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import redis.clients.jedis.Jedis;
import xin.mrbear.analysis.storm.dao.LogAnalyzerDao;
import xin.mrbear.analysis.storm.domain.LogAnalyzeJob;
import xin.mrbear.analysis.storm.domain.LogAnalyzeJobDetail;
import xin.mrbear.analysis.storm.pojo.Message;

public class LogAnalyerHandler {
    private static LogAnalyzerDao logAnalyzerDao = new LogAnalyzerDao();
    private static HashMap<String, List<LogAnalyzeJob>> jobHashMap;
    private static HashMap<String, List<LogAnalyzeJobDetail>> detailHashMap;

    static {
        loadData();
    }
    /**
     * 加载数据库中的数据保存到Storm内存中
     */
    private static void loadData() {
        List<LogAnalyzeJob> logAnalyzeJobs = logAnalyzerDao.loadJobList();
        List<LogAnalyzeJobDetail> logAnalyzeJobDetails = logAnalyzerDao.loadJobDetailList();
        // 每一种日志类型的任务单独分开
        //以任务类型为key，以任务的Job为List<Job>
        for (LogAnalyzeJob logAnalyzeJob : logAnalyzeJobs) {
            int jobType = logAnalyzeJob.getJobType();
            List<LogAnalyzeJob> jobList = jobHashMap.get(jobType+"");
            if(jobList == null){
                jobList = new ArrayList<LogAnalyzeJob>();
            }

            jobList.add(logAnalyzeJob);
            jobHashMap.put(jobType+"",jobList);
        }

        // 每个job和job的判断条件成一个map
        // jobid为key，所有的组成的list为value
        detailHashMap = new HashMap<String,List<LogAnalyzeJobDetail>>();
        for (LogAnalyzeJobDetail logAnalyzeJobDetail : logAnalyzeJobDetails) {
            int jobId = logAnalyzeJobDetail.getJobId();
            List<LogAnalyzeJobDetail> detailList = detailHashMap.get(jobId + "");
            if(detailList == null){
                detailList = new ArrayList<LogAnalyzeJobDetail>();
            }

            detailList.add(logAnalyzeJobDetail);
            detailHashMap.put(jobId+"",detailList);
        }




    }

    /**
     * 1. 获取上游发送的数据 得到一个message对象
     * 2.从数据库中加载任务信息，开始匹配并计算
     * 3.将匹配到的任务结果保存到redis中
     */
    public static void process(Message message) {
        //1.获取message中的任务类型
        String type = message.getType();
        //2.通过type获取这个消息的的所有任务信息
        List<LogAnalyzeJob> jobList = jobHashMap.get(type);
        //3.开始判断
        // 迭代 每一个job 看看是否被触发
        for (LogAnalyzeJob logAnalyzeJob : jobList) {
            //继续获得job所有条件
            String jobId = logAnalyzeJob.getJobId();
            List<LogAnalyzeJobDetail> detailList = detailHashMap.get(jobId);
            //条件中,必须所有的都被满足才会触发这个任务
            boolean isMatch = false;
            for (LogAnalyzeJobDetail logAnalyzeJobDetail : detailList) {
                //如果一个条件不满足，就中断这个job所有的条件判断
                String field = logAnalyzeJobDetail.getField();
                String compareValue = message.getCompareValue(field);
                String inputvalue = logAnalyzeJobDetail.getValue();
                if(compareValue == null){
                    return;
                }
                int compare = logAnalyzeJobDetail.getCompare();
                if(compare == 1 && compareValue.contains(inputvalue)){
                    //规则满足
                    isMatch = true;
                }
                if (compare == 2 && compareValue.equals(inputvalue)) {
                    // 规则就满足了
                    isMatch = true;
                }
                if (!isMatch) {
                    return;
                }
            }


            //如果一个job的所有任务都满足
            //pv 来一条消息就算一条

            String pvKey = "loganalyzer:"+jobId+":pv:20190808";
            Jedis jedis = RedisPool.getJedis();
            jedis.incr(pvKey);


        }
    }
}
