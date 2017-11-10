package com.tk.bigdata.java_bin.utils;

import java.io.IOException;

import com.tk.bigdata.java_bin.domain.SqoopBean;
import com.tk.bigdata.java_bin.em.SqlType;
import com.tk.bigdata.java_bin.test.Demo12_MatchDate;
import com.tk.bigdata.java_bin.test.Demo3;
import com.tk.bigdata.java_bin.test.Demo7_JDBC;

public class SqoopProcessUtils {

    public static String renderCommond(SqoopBean bean) throws IOException {
        StringBuffer sb = new StringBuffer("");
        sb.append("sqoop ")
                .append("import ")
                .append("-D mapreduce.job.queuename=exporter ")
                .append("--connect '")
                .append(bean.getUrl()).append("' ")
                .append("--username ")
                .append(bean.getUsername()).append(" ")
                .append("--password ")
                .append(bean.getPassword()).append(" ")
                .append("--query 'SELECT ")
        ;

        if (Utils.isEmpty(bean.getColums())) {
            sb
//			  .append(bean.getSchema()).append(".")
                    .append(bean.getTableName()).append(".*");
        } else {
            sb.append(bean.getColums());
        }

        if (Utils.isEmpty(bean.getRowKey())) {
            if (bean.getUrl().contains("oracle")) {
                sb.append(", MD5(SYS_GUID()) AS ROWKEY");
            } else if (bean.getUrl().contains("mysql")) {
                sb.append(", MD5(UUID()) AS ROWKEY");
            }
        } else {
            sb.append(", ").append(bean.getRowKey()).append(" AS ROWKEY");
        }

        sb
                .append(" FROM ")
//		  .append(bean.getSchema())
//		  .append(".")
                .append(bean.getTableName())
                .append(" WHERE $CONDITIONS")
        ;

        if (!Utils.isEmpty(bean.getWhere())) {
            sb.append(" AND (").append(Demo12_MatchDate.getWhereSql(bean.getWhere(), bean.getUrl().contains("oracle") ? SqlType.ORACLE : SqlType.MYSQL).replace("'", "'\"'")).append(")");
        }

        sb
                .append("' ")
                .append("--split-by '")
                .append(bean.getSplitBy().replace("'", "'\"'")).append("' ")


                .append("--hbase-table ")
                .append(bean.getHbaseTableName()).append(" ")
                .append("--column-family ")
                .append(bean.getColumnFamily()).append(" ")
                .append("--hbase-row-key ROWKEY ")
        ;

        if ("1".equals(bean.getAddRowKey())) {
            sb.append("sqoop.hbase.add.row.key=true ");
        }

        if (bean.getNumMappers() != null) {
            sb.append("--num-mappers ").append(bean.getNumMappers()).append(" ");
        }

        if (bean.getNumMappers() != null) {
            sb.append("--m ").append(bean.getNumMappers()).append(" ");
        }

        sb
                .append("-hbase-create-table ")
                //处理一下日志输出的位置
                //预处理一下commond命令, 记录sys的log位置
                .append("1>>")
                .append(ProcessUtils.SYS_PATH)
                .append(Demo3.START_DAY)
                .append("/")
//		  .append(System.currentTimeMillis())
//		  .append("-")
                .append(bean.getName())
                .append(".sys")
                .append(" 2>&1")
        ;
        FileUtils.validateFile(ProcessUtils.SYS_PATH + Demo3.START_DAY + "/");
        FileUtils.createFile(ProcessUtils.SYS_PATH + Demo3.START_DAY + "/" + bean.getName() + ".sys");

        return sb.toString();
    }

    public static String getCountSQL(SqoopBean bean) {
        StringBuffer sb = new StringBuffer("");

        sb.append("SELECT count(0) SQL_COUNT FROM ")
//		  .append(bean.getSchema())
//		  .append(".")
                .append(bean.getTableName())
        ;

        if (!Utils.isEmpty(bean.getWhere())) {
            sb.append(" WHERE ").append(Demo12_MatchDate.getWhereSql(bean.getWhere(), bean.getUrl().contains("oracle") ? SqlType.ORACLE : SqlType.MYSQL));
        }

        return sb.toString();
    }


    /**
     * Sqoop eval 命令
     *
     * @param bean
     * @return
     */
    public static String getSqoopEval(SqoopBean bean) {
//        replaceWhere(bean);

        StringBuffer sb = new StringBuffer("");
        sb.append("sqoop ")
                .append("eval ")
                .append("--connect '")
                .append(bean.getUrl()).append("' ")
                .append("--username ")
                .append(bean.getUsername()).append(" ")
                .append("--password ")
                .append(bean.getPassword()).append(" ")
                .append("--query '")
                .append(getCountSQL(bean).replace("'","'\"'")).append("'")
                ;
        return sb.toString();
    }

    /**
     * 替换 $sysday(0) $sysday(-1)
     * @param bean
     */
    private static void replaceWhere(SqoopBean bean) {
        if(bean.getUrl().contains("oracle")){
            if(bean.getWhere() == null || "".equals(bean.getWhere())){

            }else{
                bean.setWhere(bean.getWhere().replace("$sysday(0)", "to_date('$sysday(0)', 'yyyy-mm-dd hh24:mi:ss')")
                        .replace("$sysday(-1)", "to_date('$sysday(-1)', 'yyyy-mm-dd hh24:mi:ss')"));
            }

        }else{
            if(bean.getWhere() == null || "".equals(bean.getWhere())){

            }else{
                bean.setWhere(bean.getWhere().replace("$sysday(0)", "STR_TO_DATE('$sysday(0)', '%Y-%m-%d %T')")
                        .replace("$sysday(-1)", "STR_TO_DATE('$sysday(-1)', '%Y-%m-%d %T')"));
            }
        }
    }

}
