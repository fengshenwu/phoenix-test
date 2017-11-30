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

        String zk = args[0];
        try {
            java.sql.Connection con = null;
            Statement stmt = null;
            java.util.Properties info = new java.util.Properties();
            info.setProperty("phoenix.force.index", "false");

            con = DriverManager.getConnection("jdbc:phoenix:" + zk + ":2181", info);
            stmt = con.createStatement();
            ResultSet re= stmt.executeQuery("select /*+ INDEX(test TEST_INDEX_1) */ *  from test where c1 like '446204f%'");
            System.out.println(re.first());
            con.commit();
            stmt.close();
            con.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
