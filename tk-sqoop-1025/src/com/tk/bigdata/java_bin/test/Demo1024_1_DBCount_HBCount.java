package com.tk.bigdata.java_bin.test;

import com.tk.bigdata.java_bin.domain.DbBean;
import com.tk.bigdata.java_bin.domain.SqoopBean;
import com.tk.bigdata.java_bin.utils.ConfigUtils;
import com.tk.bigdata.java_bin.utils.FileUtils;
import com.tk.bigdata.java_bin.utils.SqoopProcessUtils;

import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Demo1024_1_DBCount_HBCount {
    private int threads = 4;
    private static String YML_PATH = "";

    private static PrintWriter pw = null;
    private static String basePath = null;
    private static String nowDay = null;
    private static String yesDay = null;

    private static ExecutorService executorService = null;


    private Demo1024_1_DBCount_HBCount(int threads, String ymlPath) {
        this.threads = threads;
        YML_PATH = ymlPath;
        init();
    }

    private void init() {
        try {
            executorService = Executors.newFixedThreadPool(this.threads);

            pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream("result.log", true), "UTF-8"), true);
            pw.println("\r\n==================" + getTimes() + "==================");

            System.out.printf("【%1$s】程序开始...\r\n", getTimes());
            String day = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            nowDay = day + " 00:00:00";

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(nowDay));
            calendar.add(Calendar.DAY_OF_YEAR, -1);

            yesDay = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());

            basePath = "./validate/" + day + "/";
            FileUtils.validateFile(basePath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleCount() {
        try {
            List<SqoopBean> sqoopBeans = parserYML();

            for (SqoopBean sqoopBean : sqoopBeans) {
                executorService.submit(new DBCount_HBCount_Runnable(this, sqoopBean));
            }
            executorService.shutdown();

            awaitTerminationService();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 等待结束服务
     *
     * @throws InterruptedException
     */
    private void awaitTerminationService() throws InterruptedException {
        boolean loop = true;
        do {
            //等待所有任务完成
            loop = !executorService.awaitTermination(10, TimeUnit.SECONDS);
            //阻塞，直到线程池里所有任务结束
            System.out.println(
            		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) 
            		+ ": 阻塞中! " + loop
            		/*+"  线程数："+((ThreadPoolExecutor)executorService).getActiveCount()
            		 * +"  队列等待数："+((ThreadPoolExecutor)executorService).getQueue().size()*/);
        } while (loop);
        
        System.out.println("任务完成!!!!!!!!!!");
    }

    private String getTimes() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
    }

    /**
     * 解析YML配置文件
     *
     * @throws FileNotFoundException
     */
    private List<SqoopBean> parserYML() throws FileNotFoundException {
        List<SqoopBean> ls = new ArrayList<>();
        Yaml yaml = new Yaml();
        DbBean bean = yaml.loadAs(new FileInputStream(YML_PATH), DbBean.class);
        ls = ConfigUtils.loadingConfig(bean);
        System.out.println("加载表个数: " + ls.size());
        return ls;
    }


    private class DBCount_HBCount_Runnable implements Runnable {
        private Demo1024_1_DBCount_HBCount dbCount_hbCount;
        private SqoopBean sqoopBean;

        private DBCount_HBCount_Runnable(Demo1024_1_DBCount_HBCount dbCount_hbCount, SqoopBean sqoopBean) {
            this.dbCount_hbCount = dbCount_hbCount;
            this.sqoopBean = sqoopBean;
        }

        @Override
        public void run() {
            try {
                long startTime = System.currentTimeMillis();

                Object dbCount = queryDBCount();
                Object count = queryHBaseCount();

                long endTime = System.currentTimeMillis();

                pw.println(String.format("【%1$s】【%2$s】【%3$s】【%4$s】【%5$s】【%6$s】【%7$s】【%8$s】【%9$s】【%10$s】",
                        getTimes(), this.sqoopBean.getSchema(), this.sqoopBean.getTableName(),
                        this.sqoopBean.getHbaseTableName(), dbCount, count, 
                        Integer.parseInt(String.valueOf(dbCount)) - Integer.parseInt(String.valueOf(count)), 
                        startTime, endTime, endTime - startTime));
//                pw.println(String.format("【%1$s】【%2$s】【%3$s】【%4$s】【%5$s】【%6$s】【%7$s】【%8$s】【%9$s】",
//                        getTimes(), this.sqoopBean.getSchema(), this.sqoopBean.getTableName(),
//                        this.sqoopBean.getHbaseTableName(), dbCount, count, startTime, endTime,
//                        endTime - startTime));
                pw.flush();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * 查询HBase的数据条数
         *
         * @return
         * @throws IOException
         * @throws InterruptedException
         */
        private Object queryHBaseCount() throws IOException, InterruptedException {
            //2.查出来hbasecount
            Object count = 0;
            try {
                System.out.printf("【%2$s】【%1$s】执行ShellSQL: hbase org.apache.hadoop.hbase.mapreduce.RowCounter '%1$s' \r\n", this.sqoopBean.getHbaseTableName(), getTimes());
                Process process = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", "hbase org.apache.hadoop.hbase.mapreduce.RowCounter '" + this.sqoopBean.getHbaseTableName() + "'"});
                int status = process.waitFor();
                System.out.printf("【%3$s】【%1$s】MR脚本执行状态: %2$s \r\n", this.sqoopBean.getTableName(), status, getTimes());

                String sysLog = readInputStream(process.getInputStream());
                System.out.printf("【%3$s】【%1$s】MR输出[getInputStream]: \r\n%2$s \r\n", this.sqoopBean.getTableName(), sysLog, getTimes());

                {
                    //从正常输出中获取执行条数
                    count = getRows(sysLog);
                }
                String errLog = readInputStream(process.getErrorStream());
                System.out.printf("【%3$s】【%1$s】MR输出[getErrorStream]: \r\n%2$s \r\n", this.sqoopBean.getTableName(), errLog, getTimes());
                {
                    //从错误输出中获取执行条数
                    if (count == null)
                        count = getRows(errLog);
                }
                System.out.printf("【%3$s】【%1$s】MR count: %2$s \r\n", this.sqoopBean.getTableName(), count, getTimes());

            } catch (Exception e) {
                e.printStackTrace();
            }
            return count;
        }

        /**
         * 查询DB的数量条数
         *
         * @return
         * @throws IOException
         * @throws InterruptedException
         */
        private Object queryDBCount() throws IOException, InterruptedException {
            //1.查出来现在的dbcount
            Object dbCount = 0;
            try {
                String sqoopEval = SqoopProcessUtils.getSqoopEval(sqoopBean);
                System.out.printf("【%3$s】【%1$s】执行sqoop eval脚本: %2$s \r\n", this.sqoopBean.getTableName(), sqoopEval, getTimes());
                Process process = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", sqoopEval});
                int status = process.waitFor();
                System.out.printf("【%3$s】【%1$s】sqoop eval脚本执行状态: %2$s \r\n", this.sqoopBean.getTableName(), status, getTimes());
                String sysLog = readInputStream(process.getInputStream());
                System.out.printf("【%3$s】【%1$s】sqoop eval脚本输出[getInputStream]: \r\n%2$s \r\n", this.sqoopBean.getTableName(), sysLog, getTimes());
                {
                    //从正常输出中获取执行条数
                    dbCount = getSqoopEvalRows(sysLog);
                }
                String errLog = readInputStream(process.getErrorStream());
                System.out.printf("【%3$s】【%1$s】sqoop eval脚本输出[getErrorStream]: \r\n%2$s \r\n", this.sqoopBean.getTableName(), errLog, getTimes());
                {
                    //从错误输出中获取执行条数
                    if (dbCount == null || "".equals(dbCount))
                        dbCount = getSqoopEvalRows(errLog);
                }
                System.out.printf("【%3$s】【%1$s】DB数据量: %2$s \r\n", this.sqoopBean.getTableName(), dbCount, getTimes());
            } catch (Exception e) {
                e.printStackTrace();
            }
            return dbCount;
        }


        private String keyword_AS = "SQL_COUNT";

        /***
         *  从日志中截取出db的count
         * @param str
         * @return
         */
        private String getSqoopEvalRows(String str) {
            if (null == str || "".equals(str)) return "";
            str = str.substring(str.indexOf(keyword_AS));
            String regEx = "[^0-9]";
            Pattern p = Pattern.compile(regEx);
            Matcher m = p.matcher(str);
            return m.replaceAll("").trim();
        }

        private String sStr = "Map input records=";
        private String eStr = "Map output records=";

        private String getRows(String str) {
            if (str == null) return null;
            try {
                return str.substring(str.indexOf(sStr) + sStr.length(), str.indexOf(eStr)).trim();
            } catch (Exception e) {
                return null;
            }
        }

        private String readInputStream(InputStream in) throws IOException {
            StringBuilder sb = new StringBuilder("");
            byte[] bs = new byte[1024 * 8];
            int len = 0;
            while ((len = in.read(bs)) != -1) {
                sb.append(new String(bs, 0, len));
            }
            if (in != null) in.close();
            return sb.toString();
        }
    }


    public static void main(String[] args) {
        if (args.length >= 1) {
            Demo1024_1_DBCount_HBCount dbCount_hbCount
                    = new Demo1024_1_DBCount_HBCount(4, args[0]);
            dbCount_hbCount.handleCount();
        } else {
            System.out.println("参数不匹配");
        }


//        Demo1024_1_DBCount_HBCount dbCount_hbCount
//                = new Demo1024_1_DBCount_HBCount(4, "C:\\Users\\itw_meisf\\Desktop\\tables_count_20171020.yml");
//        dbCount_hbCount.handleCount();
    }


}



