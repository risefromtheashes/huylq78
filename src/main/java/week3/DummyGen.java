package week3;

import org.fluttercode.datafactory.impl.DataFactory;
import week3.DbUtil;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class DummyGen {
    public static void main(String[] args) throws SQLException, IOException {
        DataFactory df = new DataFactory();
        long n = 5000000;
        int BATCH_SIZE = 10000;
        Connection conn = null;
        conn = DbUtil.getConnection();
        Statement stmt = conn.createStatement();
        int count = 0;
        int pkg_order = 0;
        String sql = "INSERT INTO customers_packages(pkg_order, shop_code, customer_tel, customer_tel_normalize, fullname, pkg_created" + ","+
                " pkg_modified, package_status_id, customer_province_id, customer_district_id, customer_ward_id, created, modified, is_cancel, ightk_user_id) VALUES";
        BufferedReader reader = new BufferedReader(new FileReader("/home/quanghuy/KafkaProject/huylq78/src/main/java/week3/customer_packages.csv"));
        reader.readLine();
        String line;
        while ((line = reader.readLine())!=null) {
            String []word = line.split(",");
            sql += "(" + Integer.parseInt(word[0]) + "," + "\"" + word[1] + "\"" + "," + "\"" + word[2] + "\"" + "," + "\"" + word[3] + "\"" + "," + "\"" + word[4] + "\"" + ","
                    + word[5] + "," + word[6] + ","
                    + Integer.parseInt(word[7]) + "," + Integer.parseInt(word[8]) + "," + Integer.parseInt(word[9]) + "," + Integer.parseInt(word[10]) + ","
                    + word[11] + "," + word[12] + ","
                    + Integer.parseInt(word[13]) + "," + Integer.parseInt(word[14]) + ")";
            if ((pkg_order == n - 1) || (pkg_order == BATCH_SIZE - 1 + count * BATCH_SIZE)) {
                sql += ";";
                System.out.println("execute batch: " + count++);
                stmt.executeUpdate(sql);

                sql = "INSERT INTO customers_packages(pkg_order, shop_code, customer_tel, customer_tel_normalize, fullname, pkg_created" + "," +
                        " pkg_modified, package_status_id, customer_province_id, customer_district_id, customer_ward_id, created, modified, is_cancel, ightk_user_id) VALUES";
            } else sql += ",";
            pkg_order++;
        }
        reader.close();
        conn.close();
        System.out.println(sql);



    }
}
