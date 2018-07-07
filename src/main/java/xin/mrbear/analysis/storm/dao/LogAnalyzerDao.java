package xin.mrbear.analysis.storm.dao;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import xin.mrbear.analysis.storm.domain.BaseRecord;
import xin.mrbear.analysis.storm.domain.LogAnalyzeJob;
import xin.mrbear.analysis.storm.domain.LogAnalyzeJobDetail;


/**
 * 访问数据库
 * spring jdbctemplate
 */
public class LogAnalyzerDao  extends JdbcTemplate{
    public LogAnalyzerDao(){
        ComboPooledDataSource dataSource = new ComboPooledDataSource("logAnalyzer");
        setDataSource(dataSource);
    }


    public List<LogAnalyzeJob> loadJobList() {
        String sql = "SELECT `jobId`,`jobName`,`jobType` " +
            " FROM `log_analyze`.`log_analyze_job`" +
            " WHERE STATUS= 1";
        return query(sql, new BeanPropertyRowMapper<LogAnalyzeJob>(LogAnalyzeJob.class));
    }

    public List<LogAnalyzeJobDetail> loadJobDetailList() {
        String sql = "SELECT condi.`jobId`,condi.`field`,condi.`value`,condi.`compare` " +
            " FROM `log_analyze`.`log_analyze_job` AS job " +
            " LEFT JOIN `log_analyze`.`log_analyze_job_condition` AS condi  " +
            " ON job.`jobId` = condi.`jobId` " +
            " WHERE job.`status` =1";
        return query(sql, new BeanPropertyRowMapper<LogAnalyzeJobDetail>(LogAnalyzeJobDetail.class));
    }

    public int[][] saveMinuteAppendRecord(List<BaseRecord> appendDataList) {
        String sql = "INSERT INTO `log_analyze`.`log_analyze_job_nimute_append` (`indexName`,`pv`,`uv`,`executeTime`,`createTime` ) " +
            "VALUES (?,?,?,?,?)";
        return saveAppendRecord(appendDataList, sql);
    }

    public int[][] saveHalfAppendRecord(List<BaseRecord> appendDataList) {
        String sql = "INSERT INTO `log_analyze`.`log_analyze_job_half_append` (`indexName`,`pv`,`uv`,`executeTime`,`createTime` ) " +
            "VALUES (?,?,?,?,?)";
        return saveAppendRecord(appendDataList, sql);
    }

    public int[][] saveHourAppendRecord(List<BaseRecord> appendDataList) {
        String sql = "INSERT INTO `log_analyze`.`log_analyze_job_hour_append` (`indexName`,`pv`,`uv`,`executeTime`,`createTime` ) " +
            "VALUES (?,?,?,?,?)";
        return saveAppendRecord(appendDataList, sql);
    }

    public int[][] saveDayAppendRecord(List<BaseRecord> appendDataList) {
        String sql = "INSERT INTO `log_analyze`.`log_analyze_job_day` (`indexName`,`pv`,`uv`,`executeTime`,`createTime` ) " +
            "VALUES (?,?,?,?,?)";
        return saveAppendRecord(appendDataList, sql);
    }

    public int[][] saveAppendRecord(List<BaseRecord> appendDataList, String sql) {
        return batchUpdate(sql, appendDataList, appendDataList.size(), new ParameterizedPreparedStatementSetter<BaseRecord>() {
            public void setValues(PreparedStatement ps, BaseRecord argument) throws SQLException {
                ps.setString(1, argument.getIndexName());
                ps.setInt(2, argument.getPv());
                ps.setLong(3, argument.getUv());
                ps.setTimestamp(4, new Timestamp(new Date().getTime()));
                ps.setTimestamp(5, new Timestamp(new Date().getTime()));
            }
        });
    }

    public List<BaseRecord> sumRecordValue(String startTime, String endTime) {
        String sql = "SELECT indexName,SUM(pv) AS pv,SUM(uv) AS uv FROM `log_analyze_job_nimute_append` " +
            " WHERE  executeTime BETWEEN  '" + startTime + "' AND '" +endTime+"' "+
            " GROUP BY indexName";
        System.out.println(sql);
        return query(sql
            , new BeanPropertyRowMapper<BaseRecord>(BaseRecord.class));
    }



    public static void main(String[] args) {
        LogAnalyzerDao logAnalyzerDao = new LogAnalyzerDao();
        List<LogAnalyzeJob> logAnalyzeJobs = logAnalyzerDao.loadJobList();
        System.out.println(logAnalyzeJobs);
        List<LogAnalyzeJobDetail> logAnalyzeJobDetails = logAnalyzerDao.loadJobDetailList();
        System.out.println(logAnalyzeJobDetails);

    }

}
