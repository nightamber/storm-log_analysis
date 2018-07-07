package xin.mrbear.analysis.app;


import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 计算增量的数据
 * 当前的值减去上一个时间段的值
 * 1分钟的趋势
 * 1小时的走势
 * 15分钟走势
 * 30分钟走势
 *
 */
public class LogAnalyzerAppendData {

    public static void main(String[] args) {
        //编写一个定时程序
        // 定时执行任务的 多线程 线程池 定时
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(30);
        executorService.scheduleAtFixedRate(new MyRunnable(),0,60*1000, TimeUnit.MILLISECONDS);

    }

}
