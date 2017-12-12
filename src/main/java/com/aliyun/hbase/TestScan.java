package com.aliyun.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Created by fengshen on 12/2/17.
 */
public class TestScan {
    public static void main(String[] args) {

        System.out.println(MD5Hash.getMD5AsHex("550248".getBytes()));
        try {
            Configuration conf = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(conf);
            long start = System.currentTimeMillis();
            Scan scan = new Scan();


            scan.setRowPrefixFilter(MD5Hash.getMD5AsHex("550248".getBytes()).getBytes());

            Table table = connection.getTable(TableName.valueOf("read_history:user_reading_books"));
            ResultScanner scanner = table.getScanner(scan);

            for (Result res : scanner) {
                System.out.println(ByteBufferUtils.readVLong(ByteBuffer.wrap(res.getValue("info".getBytes(), "bookId".getBytes()))));
            }
            System.out.println("time:" + (System.currentTimeMillis() - start) + "ms");

        } catch (Exception e) {
            System.out.println("end");
            e.printStackTrace();
        }
    }
}

//    Filter filter = new PrefixFilter(MD5Hash.getMD5AsHex("550248".getBytes()).getBytes());
//            scan.setFilter(filter);
//                    scan.setCaching(100);
//                    scan.setCacheBlocks(false);
//scan 'read_history:user_reading_books', {ROWPREFIXFILTER=>'fe5ee8cb54ba1451426d5457ff3bc75e'}
