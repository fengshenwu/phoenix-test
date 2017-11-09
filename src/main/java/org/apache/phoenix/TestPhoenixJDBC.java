package org.apache.phoenix;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.UUID;

/**
 * Hello world!
 */
public class TestPhoenixJDBC {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("please input zk threadnumber");
            return;
        }
        String zk = args[0];
        int m = Integer.valueOf(args[1]);


        Connection con = null;
        Statement stmt = null;

        try {
            con = DriverManager.getConnection("jdbc:phoenix:" + zk + ":2181");
            stmt = con.createStatement();

            stmt.executeUpdate("drop index  if exists  TEST_INDEX_1 on test");
            stmt.executeUpdate("drop index  if exists  TEST_INDEX_2 on test");
            stmt.executeUpdate("drop index   if exists TEST_INDEX_3 on test");
            stmt.executeUpdate("drop index   if exists  TEST_INDEX_4 on test");
            stmt.executeUpdate("drop table  if exists test ") ;
            stmt.executeUpdate("create table Test (k varchar not null primary key, c1 varchar," +
                    "c2 varchar,c3 varchar,c4 varchar,c5 varchar," +
                    "c6 varchar,c7 varchar,c8 varchar,c9 varchar,c10 varchar)  SALT_BUCKETS = 20");
            stmt.executeUpdate("CREATE INDEX TEST_INDEX_1 ON TEST(c1)  SALT_BUCKETS = 20");
            stmt.executeUpdate("CREATE INDEX TEST_INDEX_2 ON TEST(c4,c5)  SALT_BUCKETS = 20");
            stmt.executeUpdate("CREATE INDEX TEST_INDEX_3 ON TEST(c7,c8,c2)   SALT_BUCKETS = 20");
            stmt.executeUpdate("CREATE INDEX TEST_INDEX_4 ON TEST(c5,c3,c9,c1)   SALT_BUCKETS = 20");
            stmt.close();
            con.close();

            Thread t = null;
            for (int i = 0; i < m; i++) {
                t = new TestPhoenixJDBC.RunSQL(zk);
                t.start();

            }

//            t.wait();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    static class RunSQL extends Thread {
        String zk = null;

        public RunSQL(String zk) {
            this.zk = zk;
        }


        public void run() {
            {
                Connection con = null;
                Statement stmt = null;
                ResultSet rset = null;

                try {
                    con = DriverManager.getConnection("jdbc:phoenix:" + zk + ":2181");
                    stmt = con.createStatement();

                    for (int i = 1; i < 10000000; i++) {
                        long start = System.currentTimeMillis();
                        if (i % 100 == 0) {
                            con.commit();
                            long end = System.currentTimeMillis();
                            System.out.println("insert 100，time:"+(end-start)+"ms");
                        }
                        stmt.executeUpdate("upsert into Test values ('" + UUID.randomUUID()
                                + "','" + UUID.randomUUID() + "','" + UUID.randomUUID()
                                + "','" + UUID.randomUUID() + "','" + UUID.randomUUID()
                                + "','" + UUID.randomUUID() + "','" + UUID.randomUUID()
                                + "','" + UUID.randomUUID() + "','" + UUID.randomUUID()
                                + "','" + UUID.randomUUID() + "','" + UUID.randomUUID()
                                + "')");
                    }
                    con.commit();

//                    PreparedStatement statement = con.prepareStatement("select * from test");
//                    rset = statement.executeQuery();
//                    while (rset.next()) {
//                        System.out.println(rset.getString("mycolumn"));
//                    }
                    stmt.close();
                    rset.close();
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
        }
    }
}
