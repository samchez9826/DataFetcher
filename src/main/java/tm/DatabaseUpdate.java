package tm;

import java.sql.*;

public class DatabaseUpdate {

    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3307/retail?useSSL=false&serverTimezone=UTC&characterEncoding=UTF-8";
        String user = "root";
        String password = "root1234";

        // 假设要插入的表结构和tmp4查询结果一致
        String insertSQL = "INSERT INTO tm_item_final (id, num_iid, title, nick, detail_url, pic_url, brand, num, price, properties_name, quantity, sku_id, 规格, 包装, brand2, 规格2, 单盒价, if_reg) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            // 执行WITH查询并获取结果
            String withQuery = "WITH tmp AS (\n" +
                    "    SELECT id, num_iid, title, nick, detail_url, case when pic_url not like 'https:%' then concat('https:',pic_url) else pic_url end as pic_url, brand, num, price, properties_name, quantity, sku_id, sku_url," +
                    "        case when detail_url is null then concat('https://detail.tmall.com/item.htm?id=',num_iid,'&skuId=',sku_id)\n" +
                    "        else sku_url end as new_sku_url,\n" +
                    "        REGEXP_SUBSTR(title, '([0-9]+(mg|g|ml|克|丸)\\\\*[0-9]+(片|袋|支|片/盒)?)|([0-9]+(mg|g|ml|克|丸)/盒)') AS `规格`,\n" +
                    "        CASE\n" +
                    "            WHEN properties_name IS NULL THEN '1盒'\n" +
                    "            WHEN properties_name LIKE '%单盒%' THEN '1盒'\n" +
                    "            WHEN properties_name LIKE '%标准%' then '1盒'\n"+
                    "            WHEN properties_name REGEXP '[0-9]+盒' THEN \n" +
                    "                REGEXP_SUBSTR(properties_name, '[0-9]+盒')\n" +
                    "            ELSE COALESCE(\n" +
                    "                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(\n" +
                    "                    REGEXP_SUBSTR(properties_name, '[一二三四五六七八九十]+盒装'), \n" +
                    "                    '一', '1'), '二', '2'), '三', '3'), '四', '4'), '五', '5'), '十', '10'),\n" +
                    "                CONCAT(\n" +
                    "                    SUBSTRING_INDEX(\n" +
                    "                        SUBSTRING_INDEX(properties_name, '*', -1),\n" +
                    "                        '盒', 1\n" +
                    "                    ),\n" +
                    "                    '盒'\n" +
                    "                )\n" +
                    "            )\n" +
                    "        END AS `包装`,\n" +
                    "        CASE\n" +
                    "            WHEN title LIKE '%扬子江%' THEN '扬子江'\n" +
                    "            WHEN title LIKE '%贝雪%' THEN '扬子江'\n" +
                    "            WHEN title LIKE '%唐林%' THEN '扬子江'\n" +
                    "            WHEN brand LIKE '%扬子江%' THEN '扬子江'\n" +
                    "            WHEN nick LIKE '%扬子江%' THEN '扬子江'\n" +
                    "        END AS brand2\n" +
                    "    FROM tm_item_sku_combined isc\n" +
                    "    WHERE title NOT LIKE '%阿诺新%'\n" +
                    "),\n" +
                    "tmp2 AS ( \n" +
                    "    SELECT \n" +
                    "        *,\n" +
                    "        CASE\n" +
                    "            WHEN 规格 LIKE '%*%' THEN\n" +
                    "                SUBSTRING_INDEX(规格, '*', -1)\n" +
                    "            WHEN 规格 LIKE '%/盒%' THEN\n" +
                    "                '1盒'\n" +
                    "            ELSE\n" +
                    "                '1'\n" +
                    "        END AS 规格2,\n" +
                    "        ROUND(price / CAST(REGEXP_REPLACE(包装, '\\\\D', '') AS DECIMAL(10,2)), 2) AS 单盒价\n" +
                    "    FROM \n" +
                    "        tmp\n" +
                    "),\n" +
                    "tmp3 AS (\n" +
                    "    SELECT \n" +
                    "        *,\n" +
                    "        CASE\n" +
                    "            WHEN title LIKE '%蓝芩口服液%' AND SUBSTRING_INDEX(规格2, '支', 1) IN ('7', '9', '12') THEN '店铺不合规'\n" +
                    "            WHEN title LIKE '%黄芪精%' AND SUBSTRING_INDEX(规格2, '支', 1) IN ('60', '30', '24','18') THEN '店铺不合规'\n" +
                    "            WHEN title LIKE '%可乐可康%' AND SUBSTRING_INDEX(规格2, '支', 1) IN ('12', '18', '36') THEN '店铺不合规'\n" +
                    "            WHEN title LIKE '%星迪%' AND SUBSTRING_INDEX(规格2, '片', 1) IN ('28', '21') THEN '店铺不合规'\n" +
                    "            WHEN title LIKE '%贝雪%' AND SUBSTRING_INDEX(规格2, '片', 1) IN ('5') THEN '店铺不合规'\n" +
                    "            WHEN title LIKE '%补肾润肺%' AND SUBSTRING_INDEX(规格2, '瓶', 1) IN ('3') THEN '店铺不合规'\n" +
                    "            WHEN title LIKE '%苏黄%' AND SUBSTRING_INDEX(规格2, '粒', 1) IN ('9') THEN '店铺不合规'\n" +
                    "            WHEN (title LIKE '%依帕司他片（唐林）%' OR title LIKE '%唐林胶囊%') AND SUBSTRING_INDEX(规格2, '片', 1) IN ('18') THEN '店铺不合规'\n" +
                    "            WHEN title LIKE '%散风通窍滴丸%' AND SUBSTRING_INDEX(规格2, '丸', 1) IN ('60') THEN '店铺不合规'\n" +
                    "            WHEN title LIKE '%杏荷止咳%' AND SUBSTRING_INDEX(规格2, '瓶', 1) IN ('1') THEN '店铺不合规'\n" +
                    "            ELSE '合规'\n" +
                    "        END AS if_rug\n" +
                    "    FROM \n" +
                    "        tmp2\n" +
                    "),\n" +
                    "tmp4 AS (\n" +
                    "    SELECT \n" +
                    "        id, num_iid, title, nick, new_sku_url, pic_url, brand, num, price, properties_name, quantity, sku_id, 规格, 包装, brand2, 规格2, 单盒价,\n" +
                    "        CASE\n" +
                    "            WHEN title LIKE '%蓝芩口服液%' AND SUBSTRING_INDEX(规格2, '支', 1) = 6 and 单盒价 < 31.5 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%蓝芩口服液%' AND SUBSTRING_INDEX(规格2, '支', 1) = 12 and 单盒价 < 63.504 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%蓝芩口服液%' AND SUBSTRING_INDEX(规格2, '支', 1) = 14 and 单盒价 < 65.86 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%黄芪精%' AND SUBSTRING_INDEX(规格2, '支', 1) = 6 and 单盒价 < 22.5 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%黄芪精%' AND SUBSTRING_INDEX(规格2, '支', 1) = 12 and 单盒价 < 36 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%可乐可康%' AND SUBSTRING_INDEX(规格2, '支', 1) = 30 and 单盒价 < 72 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%香芍颗粒%' AND 规格2 like '%4g×3袋|代%' and 单盒价 < 50.4 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%香芍颗粒%' AND 规格2 like '%4g×9袋|代%' and 单盒价 < 135 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%香芍颗粒%' AND 规格2 like '%4g×12袋|代%' and 单盒价 < 201.6 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%神曲消食%' AND SUBSTRING_INDEX(规格2, '支', 1) = 6 and 单盒价 < 64.8 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%神曲消食%' AND SUBSTRING_INDEX(规格2, '支', 1) = 18 and 单盒价 < 50.256 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%神曲消食%' AND SUBSTRING_INDEX(规格2, '支', 1) = 24 and 单盒价 < 67.005 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%贝雪%' AND SUBSTRING_INDEX(规格2, '片', 1) = 6 and 单盒价 < 55.125 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%贝雪%' AND SUBSTRING_INDEX(规格2, '片', 1) = 18 and 单盒价 < 28.8 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%百安新%' AND 规格2 like '%12.5mg×7%' and 单盒价 < 37.35 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%依帕司他片（唐林）%' AND SUBSTRING_INDEX(规格2, '片', 1) = 10 and 单盒价 < 30.96 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%唐林胶囊%' AND SUBSTRING_INDEX(规格2, '粒', 1) = 10 and 单盒价 < 33.33 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%荜铃胃痛%' AND SUBSTRING_INDEX(规格2, '袋|代', 1) = 6 and 单盒价 < 27.96 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%荜铃胃痛%' AND SUBSTRING_INDEX(规格2, '袋|代', 1) = 18 and 单盒价 < 41.94 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%散风通窍%' AND SUBSTRING_INDEX(规格2, '丸', 1) = 100 and 单盒价 < 77.4 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%卢苏%' AND SUBSTRING_INDEX(规格2, '片', 1) = 3 and 单盒价 < 48.609 THEN '价格不合规'\n" +
                    "            WHEN title LIKE '%卢苏%' AND SUBSTRING_INDEX(规格2, '片', 1) = 5 and 单盒价 < 81.018 THEN '价格不合规'\n" +
                    "            ELSE '合规'\n" +
                    "        END AS if_reg\n" +
                    "    FROM \n" +
                    "        tmp3\n" +
                    ")\n" +
                    "SELECT \n" +
                    "    * \n" +
                    "FROM \n" +
                    "    tmp4" +
                    ";\n"; // 填入您的完整SQL
            PreparedStatement withStmt = conn.prepareStatement(withQuery);
            ResultSet rs = withStmt.executeQuery();

            // 准备插入语句
            PreparedStatement insertStmt = conn.prepareStatement(insertSQL);

            while (rs.next()) {
                // 假设列id, num_iid等是查询结果的列名
                insertStmt.setInt(1, rs.getInt("id"));
                insertStmt.setString(2, rs.getString("num_iid"));
                insertStmt.setString(3, rs.getString("title"));
                insertStmt.setString(4, rs.getString("nick"));
                insertStmt.setString(5, rs.getString("new_sku_url"));
                insertStmt.setString(6, rs.getString("pic_url"));
                insertStmt.setString(7, rs.getString("brand"));
                insertStmt.setString(8, rs.getString("num"));
                insertStmt.setString(9, rs.getString("price"));
                insertStmt.setString(10, rs.getString("properties_name"));
                insertStmt.setString(11, rs.getString("quantity"));
                insertStmt.setString(12, rs.getString("sku_id"));
                insertStmt.setString(13, rs.getString("规格"));
                insertStmt.setString(14, rs.getString("包装"));
                insertStmt.setString(15, rs.getString("brand2"));
                insertStmt.setString(16, rs.getString("规格2"));
                insertStmt.setString(17, rs.getString("单盒价"));
                insertStmt.setString(18, rs.getString("if_reg"));
                insertStmt.executeUpdate();
            }

            System.out.println("Data inserted successfully.");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
