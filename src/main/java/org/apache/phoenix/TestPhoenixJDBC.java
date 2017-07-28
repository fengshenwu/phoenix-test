package org.apache.phoenix;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * Hello world!
 */
public class TestPhoenixJDBC {
    public static void main(String[] args) {
        Connection con = null;
        Statement stmt = null;
        ResultSet rset = null;

        try {
            con = DriverManager.getConnection("jdbc:phoenix:hb-bp155o277lccd8ae0-001.hbase.rds.aliyuncs.com");
            stmt = con.createStatement();

//            stmt.executeUpdate("create table test (mykey integer not null primary key, mycolumn varchar)");
            stmt.executeUpdate("upsert into test values (1,'Hello')");
            stmt.executeUpdate("upsert into test values (2,'World!')");
            con.commit();

            PreparedStatement statement = con.prepareStatement("select * from test");
            rset = statement.executeQuery();
            while (rset.next()) {
                System.out.println(rset.getString("mycolumn"));
            }
            stmt.close();
            rset.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
