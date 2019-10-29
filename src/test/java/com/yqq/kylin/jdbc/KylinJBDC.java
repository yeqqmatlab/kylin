package com.yqq.kylin.jdbc;

import org.apache.kylin.jdbc.Driver;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * java jdbc kylin
 * Created by yqq on 2019/10/29.
 */
public class KylinJBDC {

    public static void main(String[] args) throws Exception {

        StringBuffer sql = new StringBuffer();
        sql.append(" SELECT ");
        sql.append(" 	school_id, ");
        sql.append(" 	school_name, ");
        sql.append(" 	sum(is_feedback) AS sum_feedback, ");
        sql.append(" 	sum(IS_THREE_ISP_DOWNLOAD) + sum(IS_TWO_ISP_DOWNLOAD) + sum(IS_WRONGBOOK_DOWNLOAD) AS download_sum, ");
        sql.append(" 	COUNT(DISTINCT exam_group_id) AS exam_group_count, ");
        sql.append(" 	COUNT(DISTINCT student_id) AS distinct_student_sum, ");
        sql.append(" 	count(student_id) AS student_sum ");
        sql.append(" FROM ");
        sql.append(" 	dws_student_paper_exam  ");
        sql.append(" WHERE ");
        sql.append(" 	school_name like '%中学%' ");
        sql.append(" GROUP BY ");
        sql.append(" 	school_id, ");
        sql.append(" 	school_name ");
        sql.append(" HAVING ");
        sql.append(" 	count(student_id) > 1200  ");
        sql.append(" 	and COUNT(DISTINCT exam_group_id) > 5 and COUNT(DISTINCT exam_group_id) < 100 ");

        final ResultSet resultSet = queryKylin(sql.toString(), "zsy_data_coach_stat");
        System.out.println("resultSet--->"+resultSet);
        while (resultSet.next()){

            int schoolId = resultSet.getInt(1);
            String schoolName = resultSet.getString(2);
            int feedBackSum = resultSet.getInt(3);
            int downloadSum = resultSet.getInt(4);
            int examSum = resultSet.getInt(5);
            int distinctStudentSum = resultSet.getInt(6);
            int studentSum = resultSet.getInt(7);
            System.out.println("res-->"+schoolId+"--"+schoolName+"--"+feedBackSum+"--"+downloadSum+"--"+examSum+"--"+distinctStudentSum+"--"+studentSum);
        }
    }

    public static ResultSet queryKylin(String sql, String projectName) throws Exception {
        ResultSet resultSet=null;
        Connection conn=null;
        try {
            System.setProperty("user.timezone","GMT +08");
            // 加载Kylin的JDBC驱动程序
            Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
            // 配置登录Kylin的用户名和密码
            Properties info= new Properties();
            info.put("user","ADMIN");
            info.put("password","KYLIN");
            // 连接Kylin服务
            String connectStr="jdbc:kylin://ip244:7070/"+projectName;
            conn= driver.connect(connectStr, info);
            Statement state= conn.createStatement();
            if(sql!=null && !"".equals(sql)){
                System.out.println(projectName+"===="+sql);
                resultSet =state.executeQuery(sql);
            }else{
                System.out.println("=======sql为空=======");
            }
        }catch (Exception e){
            throw new Exception("连接kylin报错======"+e.getMessage());
        }finally {
            if(conn!=null){
                conn.close();
            }
            return  resultSet;
        }
    }

}
