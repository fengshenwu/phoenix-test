package com.aliyun.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by fengshen on 11/11/17.
 */
public class TestHBase {

    private static final String TABLE_NAME = "tp";
    private static final String CF_DEFAULT = "f1";
    public static final byte[] QUALIFIER = "c1".getBytes();

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("please input zk threadnumber");
            return;
        }
        String zk = args[0];
        int m = Integer.valueOf(args[1]);
        Statistics statistics = new Statistics();
        statistics.start();
        Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, zk);
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(config);
            connection.getAdmin().disableTable(TableName.valueOf(TABLE_NAME));
            connection.getAdmin().deleteTable(TableName.valueOf(TABLE_NAME));
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            tableDescriptor.addFamily(new HColumnDescriptor(CF_DEFAULT));
            System.out.print("Creating table. ");
            Admin admin = connection.getAdmin();
            admin.createTable(tableDescriptor);
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
        for (int i = 0; i < m; i++) {
            Run r = new Run(zk, statistics);
            r.run();
        }


    }

    static class Run extends Thread {
        String zk = null;

        Statistics statistics;

        public Run(String zk, Statistics statistics) {
            this.zk = zk;
            this.statistics = statistics;

        }


        public void run() {
            Configuration config = HBaseConfiguration.create();
            config.set(HConstants.ZOOKEEPER_QUORUM, zk);
            Connection connection = null;
            try {
                connection = ConnectionFactory.createConnection(config);
                Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

                while (true) {
                    long start = System.nanoTime();
                    long end = System.nanoTime();
                    statistics.put(end - start);
                    start = System.nanoTime();;
                    List<Put> puts = new ArrayList<Put>();
                    for (int j = 0; j < 100; j++) {
                        Put put = new Put(UUID.randomUUID().toString().getBytes());
                        put.addColumn(CF_DEFAULT.getBytes(), QUALIFIER, UUID.randomUUID().toString().getBytes());
                        puts.add(put);
                    }
                    table.put(puts);
                }
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

    }
}
