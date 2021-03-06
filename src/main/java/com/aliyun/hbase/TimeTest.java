package com.aliyun.hbase;

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

/**
 * Created by fengshen on 11/18/17.
 */
public class TimeTest {

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
        if ("drop".equals(args[2])) {
            Connection connection = null;
            try {
                connection = ConnectionFactory.createConnection(config);
                if (connection.getAdmin().tableExists(TableName.valueOf(TABLE_NAME))) {
                    System.out.println("disableTable & deleteTable table. ");
                    connection.getAdmin().disableTable(TableName.valueOf(TABLE_NAME));
                    connection.getAdmin().deleteTable(TableName.valueOf(TABLE_NAME));
                }
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
                tableDescriptor.addFamily(new HColumnDescriptor(CF_DEFAULT));
                System.out.println("Creating table. ");
                Admin admin = connection.getAdmin();
                ArrayList<String> splits = GenHash.genHash(regions);
                byte[][] splitKeys = new byte[splits.size()][];
                for (int i = 0; i < splits.size(); i++) {
                    splitKeys[i] = splits.get(i).getBytes();
                }
                admin.createTable(tableDescriptor, splitKeys);
                System.out.println(" Done.");
                System.out.println(UUID.randomUUID().toString().getBytes().length);

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
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
            TimeTest.Run r = new TimeTest.Run(zk, statistics, executor, conf);
            r.start();
        }
        try {
            Thread.sleep(1000000000);
            System.out.println("exit");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

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

            Connection connection = null;
            try {
                connection = ConnectionFactory.createConnection(conf, executor);

                Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
                long start = System.currentTimeMillis();
                while (true) {
                    List<Put> puts = new ArrayList<Put>();
                    for (int j = 0; j < 1; j++) {
                        Put put = new Put(UUID.randomUUID().toString().getBytes());
                        put.addColumn(CF_DEFAULT.getBytes(), QUALIFIER, getRandomString(1024).getBytes());
                        puts.add(put);
                    }
                    table.put(puts);
                    long end = System.currentTimeMillis();
                    statistics.put(end - start);
                    start = System.currentTimeMillis();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println("thread exit");
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

    }
}
