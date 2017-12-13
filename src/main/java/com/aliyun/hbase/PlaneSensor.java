package com.aliyun.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.phoenix.GenHash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Created by fengshen on 12/11/17.
 */
public class PlaneSensor {


    private static final String TABLE_NAME = "ps";
    private static final String CF_DEFAULT = "f";
    public static final byte[] QUALIFIER = "q".getBytes();

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("please input zk threadnumber i");
            return;
        }
        String zk = args[0];
        int size = Integer.valueOf(args[2]);
        int m = Integer.valueOf(args[1]);
        System.out.println("m:" + m);
        Statistics statistics = new Statistics();
        statistics.start();
        Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, zk);
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(config);
            if (connection.getAdmin().tableExists(TableName.valueOf(TABLE_NAME))) {
                if (connection.getAdmin().isTableEnabled(TableName.valueOf(TABLE_NAME))) {

                    connection.getAdmin().disableTable(TableName.valueOf(TABLE_NAME));
                }
                connection.getAdmin().deleteTable(TableName.valueOf(TABLE_NAME));
            }

            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            tableDescriptor.addFamily(new HColumnDescriptor(CF_DEFAULT));
            System.out.print("Creating table. ");
            Admin admin = connection.getAdmin();

            ArrayList<String> splits = GenHash.genHash(100);
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
        for (int i = 0; i < m; i++) {
            PlaneSensor.Run r = new PlaneSensor.Run(zk, statistics, size);
            r.start();
        }


    }

    static class Run extends Thread {
        String zk = null;

        int size = 0;

        Statistics statistics;

        public Run(String zk, Statistics statistics, int size) {
            this.zk = zk;
            this.statistics = statistics;
            this.size = size;

        }

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


        public void run() {
            Configuration config = HBaseConfiguration.create();
            config.set(HConstants.ZOOKEEPER_QUORUM, zk);
            Connection connection = null;
            System.out.println("add thread");

            try {
                connection = ConnectionFactory.createConnection(config);
                Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

                while (true) {
                    long start = System.nanoTime();
                    List<Put> puts = new ArrayList<Put>();
                    int p = 0;
                    for (int j = 0; j < 40000; j++) {
                        String planeNo = "plane1";
                        String SensorNo = String.valueOf(j);
                        String rowkey = MD5Hash.getMD5AsHex((planeNo + SensorNo).getBytes()).substring(0, 5) + planeNo + SensorNo + "j" + System.currentTimeMillis();
                        Put put = new Put(rowkey.getBytes());
                        String value = getRandomString(size); // double double double double double  1000个 压缩
                        put.addColumn(CF_DEFAULT.getBytes(), QUALIFIER, value.getBytes());
                        puts.add(put);
                        if (p++ % 500 == 0) {
                            System.out.println("add 11");
                            table.put(puts);
                        }
                    }

                }
            } catch (
                    Exception e)

            {
                e.printStackTrace();
            } finally

            {
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
