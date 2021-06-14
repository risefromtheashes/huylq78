package week3;

import org.fluttercode.datafactory.impl.DataFactory;
import week3.DbUtil;

import java.sql.*;

public class DummyGen {
    public static void main(String[] args) throws SQLException {
        DataFactory df = new DataFactory();
        long n = 5000000;
        int BATCH_SIZE = 10000;
        Connection conn = null;
        conn = DbUtil.getConnection();
        Statement stmt = conn.createStatement();
        int count = 0;
        String sql = "INSERT INTO customers_packages(pkg_order, shop_code, customer_tel, customer_tel_normalize, fullname, pkg_created" + ","+
                " pkg_modified, package_status_id, customer_province_id, customer_district_id, customer_ward_id, created, modified, is_cancel, ightk_user_id) VALUES";
        for (int i = 0; i < n; i++) {
            sql += "(" + i + ","  + "\"" + df.getRandomChars(1) + "\"" + "," + "\"" + df.getNumberText(10) + "\"" + "," + "\"" + df.getNumberText(10) + "\"" + "," + "\"" +df.getFirstName() + " " + df.getLastName() + "\"" + ","
                    + "CURRENT_TIMESTAMP" + "," + "CURRENT_TIMESTAMP" + ","
                    + df.getNumberBetween(1,2) + "," + df.getNumberBetween(1,99) + "," + df.getNumberBetween(1,99) + "," + df.getNumberBetween(1,99) + ","
                    + "CURRENT_TIMESTAMP" + "," + "CURRENT_TIMESTAMP" + ","
                    + df.getNumberBetween(1,2) + "," + df.getNumberBetween(1,99) + ")";
            if((i == n-1)|| (i == BATCH_SIZE-1+count*BATCH_SIZE)){
                sql += ";";
                System.out.println("execute batch"+ count++);
                    stmt.executeUpdate(sql);

                sql = "INSERT INTO customers_packages(pkg_order, shop_code, customer_tel, customer_tel_normalize, fullname, pkg_created" + ","+
                        " pkg_modified, package_status_id, customer_province_id, customer_district_id, customer_ward_id, created, modified, is_cancel, ightk_user_id) VALUES";
            }
            else sql += ",";
        }

        System.out.println(sql);



    }
}
