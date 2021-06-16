package week3;

import org.fluttercode.datafactory.impl.DataFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class DummyGenFile {
    public static void main(String[] args) throws SQLException, IOException {
        DataFactory df = new DataFactory();
        long n = 5000000;
        int BATCH_SIZE = 10000;
        Connection conn = null;
        conn = DbUtil.getConnection();
        Statement stmt = conn.createStatement();
        int count = 0;
        int pkg_order, pkg_status_id, customer_province_id, customer_district_id, customer_ward_id,is_cancel, ightk_user_id;
        String shop_code, customer_tel, customer_tel_normalize, fullname, CURRENT_TIMESTAMP;
        String sql = "INSERT INTO customers_packages(pkg_order, shop_code, customer_tel, customer_tel_normalize, fullname, pkg_created" + ","+
                " pkg_modified, package_status_id, customer_province_id, customer_district_id, customer_ward_id, created, modified, is_cancel, ightk_user_id) VALUES";
        String str = "Hello";
        BufferedWriter writer = new BufferedWriter(new FileWriter("/home/quanghuy/KafkaProject/huylq78/src/main/java/week3/customer_packages.csv"));
        writer.write("pkg_order,shop_code,customer_tel,customer_tel_normalize,fullname,pkg_created,pkg_modified,package_status_id,customer_province_id,customer_district_id,customer_ward_id,created,modified,is_cancel,ightk_user_id\n");

        for (int i = 0; i < n; i++) {
            pkg_order = i;
            shop_code = df.getRandomChars(1);
            customer_tel = df.getNumberText(10);
            customer_tel_normalize = df.getNumberText(10);
            fullname = df.getFirstName() + " " + df.getLastName();
            pkg_status_id = df.getNumberBetween(1,2);
            customer_province_id = df.getNumberBetween(1,99);
            customer_district_id = df.getNumberBetween(1,99);
            customer_ward_id = df.getNumberBetween(1,99);
            is_cancel = df.getNumberBetween(1,2);
            ightk_user_id = df.getNumberBetween(1,2);

            writer.write(pkg_order+","+shop_code+","+customer_tel+","+customer_tel_normalize+","+fullname+","+"CURRENT_TIMESTAMP"+","+"CURRENT_TIMESTAMP"+","+pkg_status_id+","+customer_province_id+","+customer_district_id+","+customer_ward_id+","+"CURRENT_TIMESTAMP"+","+"CURRENT_TIMESTAMP"+","+is_cancel+","+ightk_user_id+"\n");
//            sql += "(" + pkg_order + ","  + "\"" + shop_code + "\"" + "," + "\"" + customer_tel + "\"" + "," + "\"" + customer_tel_normalize + "\"" + "," + "\"" + fullname + "\"" + ","
//                    + "CURRENT_TIMESTAMP" + "," + "CURRENT_TIMESTAMP" + ","
//                    + pkg_status_id + "," + customer_province_id + "," + customer_district_id + "," + customer_ward_id + ","
//                    + "CURRENT_TIMESTAMP" + "," + "CURRENT_TIMESTAMP" + ","
//                    + is_cancel + "," + ightk_user_id + ")";

        }
        writer.close();
        System.out.println(sql);
    }
}
