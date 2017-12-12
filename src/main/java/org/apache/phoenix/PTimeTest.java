package org.apache.phoenix;

import com.aliyun.hbase.TimeTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.phoenix.GenHash;
import org.apache.hadoop.hbase.HConstants;
import com.aliyun.hbase.Statistics;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by fengshen on 11/18/17.
 */
public class PTimeTest {

    private static final String TABLE_NAME = "test_hbase";
    private static final String CF_DEFAULT = "f1";
    public static final byte[] QUALIFIER = "c1".getBytes();

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.out.println("please input zk threadnumber  drop regions  ");
            return;
        }
        String zk = args[0];
        int m = Integer.valueOf(args[1]);
        int regions = Integer.valueOf(args[3]);

        Statistics statistics = new Statistics();
        statistics.start();
        Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, zk);
        try {

            if ("drop".equals(args[2])) {

                java.sql.Connection con = null;
                Statement stmt = null;
                java.util.Properties info = new java.util.Properties();
                info.setProperty("phoenix.force.index", "false");
                con = DriverManager.getConnection("jdbc:phoenix:" + zk + ":2181", info);
                stmt = con.createStatement();

                stmt.executeUpdate("drop index  if exists  TEST1_INDEX_1 on test1");
                stmt.executeUpdate("drop index  if exists  TEST1_INDEX_2 on test1");
                stmt.executeUpdate("drop index   if exists TEST1_INDEX_3 on test1");
                stmt.executeUpdate("drop index   if exists  TEST1_INDEX_4 on test1");
                stmt.executeUpdate("drop table  if exists test1 ");
                stmt.executeUpdate("create table Test1 (k varchar not null primary key, c1 varchar," +
                        "c2 varchar,c3 varchar,c4 varchar,c5 varchar," +
                        "c6 varchar,c7 varchar,c8 varchar,c9 varchar,c10 varchar)  SALT_BUCKETS = " + 245);
                stmt.executeUpdate("CREATE INDEX TEST1_INDEX_1 ON TEST1(c1)  SALT_BUCKETS = " + 245);
                stmt.executeUpdate("CREATE INDEX TEST1_INDEX_2 ON TEST1(c2)  SALT_BUCKETS = " + 245);
                stmt.executeUpdate("CREATE INDEX TEST1_INDEX_3 ON TEST1(c3)   SALT_BUCKETS = " + 245);
                stmt.executeUpdate("CREATE INDEX TEST1_INDEX_4 ON TEST1(c4)   SALT_BUCKETS = " + 245);
                stmt.executeUpdate("CREATE INDEX TEST1_INDEX_5 ON TEST1(c5)   SALT_BUCKETS = " + 245);
                stmt.executeUpdate("CREATE INDEX TEST1_INDEX_6 ON TEST1(c6)   SALT_BUCKETS = " + 245);
                stmt.executeUpdate("CREATE INDEX TEST1_INDEX_7 ON TEST1(c7)   SALT_BUCKETS = " + 245);
                stmt.executeUpdate("CREATE INDEX TEST1_INDEX_8 ON TEST1(c8)   SALT_BUCKETS = " + 245);
                stmt.executeUpdate("CREATE INDEX TEST1_INDEX_9 ON TEST1(c9)   SALT_BUCKETS = " + 245);


                stmt.close();
                con.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, zk);
        conf.set(HConstants.HBASE_RPC_TIMEOUT_KEY, "30");
        conf.set(HConstants.HBASE_RPC_TIMEOUT_KEY, "30");
        conf.set("hbase.client.pause", "50");
        conf.set("hbase.client.retries.number", "30");
        conf.set("hbase.rpc.timeout", "2000");
        conf.set("hbase.client.operation.timeout", "3000");
        conf.set("hbase.client.scanner.timeout.period", "10000");
        ExecutorService executor = Executors.newFixedThreadPool(500);
        for (int i = 0; i < m; i++) {
            PTimeTest.Run r = new PTimeTest.Run(zk, statistics, executor, conf);
            r.start();
        }
        try

        {
            while (true) Thread.sleep(1000000000);
        } catch (
                InterruptedException e)

        {
            e.printStackTrace();
        }
        System.out.println("1exit");

    }

    static class Run extends Thread {
        String zk = null;
        Statistics statistics;
        ExecutorService executor;
        Configuration conf;

        public static String getRandomString(int length) {
            String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            Random random = new Random();
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < length; i++) {
                int number = random.nextInt(62);
                sb.append(str.charAt(number));
            }
            return sb.toString();
        }

        public Run(String zk, Statistics statistics, ExecutorService executor, Configuration conf) {
            this.zk = zk;
            this.statistics = statistics;
            this.executor = executor;
            this.conf = conf;
        }

        public void run() {

            try {
                java.sql.Connection con = null;
                Statement stmt = null;
                con = DriverManager.getConnection("jdbc:phoenix:" + zk + ":2181");
                stmt = con.createStatement();

//                stmt.executeUpdate("select /*+ INDEX(test TEST_INDEX_1) */ *  from test where c1 like '546204f%'");

                int i = 0;
                while (true) {
                    long start = System.currentTimeMillis();
                    stmt.executeUpdate("upsert into Test1 values ('" + UUID.randomUUID()
                            + "','" + UUID.randomUUID() + "','" + UUID.randomUUID()
                            + "','" + UUID.randomUUID() + "','" + UUID.randomUUID()
                            + "','" + UUID.randomUUID() + "','" + UUID.randomUUID()
                            + "','" + UUID.randomUUID() + "','" + UUID.randomUUID()
                            + "','" + UUID.randomUUID() + "','" + UUID.randomUUID()
                            + "')");
                    con.commit();
                    statistics.put(System.currentTimeMillis() - start);

                }

            } catch (Throwable e) {
                System.out.println("ex");
                e.printStackTrace();
            } finally {
                System.out.println("thread exit");
            }
        }

    }
}
