package org.apache.phoenix;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.io.MD5Hash;
import sun.security.provider.MD5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.phoenix.GenHash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Iterator;
import java.util.UUID;

/**
 * Hello world!
 */
public class TestPhoenixJDBC {
    public static void main(String[] args) {

        try {
            Configuration conf = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(conf);
            Scan scan = new Scan();
            Filter filter = new PrefixFilter("fe5ee8cb54ba1451426d5457ff3bc75e".getBytes());
            scan.setFilter(filter);

            Table table = connection.getTable(TableName.valueOf("read_history:user_reading_books_log"));
            ResultScanner scanner = table.getScanner(scan);

            Iterator<Result> iterator = scanner.iterator();
            while (iterator.hasNext()) {
                Result result = iterator.next();
                System.out.println(result.getRow());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
//        log.warn("卡在这里了吗1。。。。");
//        Scan scan = new Scan();
//        Filter filter = new PrefixFilter(MD5.md5(String.valueOf(userId)).getBytes());
//        scan.setFilter(filter);
//        ResultScanner scanner;
//        try {
//            scanner = table.getScanner(scan);
//        } catch (IOException e) {
//            e.printStackTrace();
//            return null;
//        } finally {
//            try {
//                if (null != table)
//                    table.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//        log.warn("卡在这里了吗2。。。。");
//        if (null != scanner) {
//            log.warn("循环里面1。。。。");
//            Iterator<Result> iterator = scanner.iterator();
//            log.warn("循环里面1.0。。。。");
//            while (iterator.hasNext()) {
//                log.warn("循环里面1.1。。。。");
//                Result result = iterator.next();


//        String zk = args[0];
//        try {
//            java.sql.Connection con = null;
//            Statement stmt = null;
//            java.util.Properties info = new java.util.Properties();
//            info.setProperty("phoenix.force.index", "false");
//
//            con = DriverManager.getConnection("jdbc:phoenix:" + zk + ":2181", info);
//            stmt = con.createStatement();
//            ResultSet re= stmt.executeQuery("select /*+ INDEX(test TEST_INDEX_1) */ *  from test where c1 like '446204f%'");
//            System.out.println(re.first());
//            con.commit();
//            stmt.close();
//            con.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}
