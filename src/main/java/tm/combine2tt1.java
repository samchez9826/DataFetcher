package tm;

import java.sql.*;

public class combine2tt1 {

    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3307/retail?useSSL=false&serverTimezone=UTC&characterEncoding=UTF-8";
        String user = "root";
        String password = "root1234";

        // 假设要插入的表结构和tmp4查询结果一致
        String insertSQL = "INSERT INTO retail.tm_new_add_detial (平台, id, num_iid, title, name, nick, detail_url, pic_url, brand_clean, num, price, 单盒价, properties_name, quantity, sku_id, 规格, 包装, brand2, 规格2, if_reg, snap_url, 省区, 城市, 商业公司) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);\n";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            // 执行WITH查询并获取结果
            String withQuery = "select\n" +
                    "  a.平台,\n" +
                    "  id,\n" +
                    "  num_iid,\n" +
                    "  title,\n" +
                    "  name,\n" +
                    "  nick,\n" +
                    "  detail_url,\n" +
                    "  pic_url,\n" +
                    "  brand_clean,\n" +
                    "  num,\n" +
                    "  -- 根据if_reg字段的值调整price和单盒价\n" +
                    "  CASE\n" +
                    "    WHEN if_reg = '店铺不合格' THEN '在售'\n" +
                    "    ELSE price\n" +
                    "  END AS price,\n" +
                    "  CASE\n" +
                    "    WHEN if_reg = '店铺不合格' THEN '在售'\n" +
                    "    ELSE 单盒价\n" +
                    "  END AS 单盒价,\n" +
                    "  properties_name,\n" +
                    "  quantity,\n" +
                    "  sku_id,\n" +
                    "  规格,\n" +
                    "  包装,\n" +
                    "  case when length(brand2)<1 then '扬子江' when brand2 is null then '扬子江' else brand2 end as brand2,\n" +
                    "  case when title like '%散风%' and 规格2 like '%5袋%' then '100丸' \n" +
                    "\t   when title like '%散风%' and 规格2 like '%3袋%' then '60丸' \n" +
                    "\t   else 规格2 end as 规格2,\n" +
                    "  case when if_reg = '店铺不合格' then '不合格' else if_reg end as if_reg,\n" +
                    "  snap_url,\n" +
                    "  clb.*\n" +
                    "  from (select \n" +
                    "    '天猫' as 平台,\n" +
                    "id,\n" +
                    "num_iid,\n" +
                    "case when title like '%苏黄止咳胶囊%' then '苏黄止咳胶囊'\n" +
                    "\t when title like '%蓝芩口服液%' then '蓝芩口服液'\n" +
                    "\t when title like '%黄芪精%' then '黄芪精'\n" +
                    "\t when title like '%依帕司他胶囊%' then '唐林胶囊'\n" +
                    "\t when title like '%依帕司他片%' then '唐林'\n" +
                    "\t when title like '%贝雪%' then '贝雪'\n" +
                    "\t when title like '%荜铃%' then '荜铃'\n" +
                    "\t when title like '%神曲消食口服液%' then '神曲消食口服液'\n" +
                    "\t when title like '%百安新%' then '百安新'\n" +
                    "\t when title like '%可乐可康%' then '可乐可康'\n" +
                    "\t when title like '%星迪%' then '星迪'\n" +
                    "\t when title like '%枸橼酸他莫昔芬片%' then '枸橼酸他莫昔芬片'\n" +
                    "\t when title like '%香芍颗粒%' then '香芍颗粒'\n" +
                    "\t when title like '%杏荷止咳糖浆%' then '杏荷止咳糖浆'\n" +
                    "\t when title like '%补肾润肺口服液%' then '补肾润肺口服液'\n" +
                    "\t when title like '%散风通窍滴丸%' then '散风通窍滴丸'\n" +
                    "\t when title like '%卢苏%' then '卢苏'\n" +
                    "\t else concat('其他品牌_',title) end as title,\n" +
                    "\t title as name,\n" +
                    "nick,\n" +
                    "detail_url,\n" +
                    "pic_url,\n" +
                    "case when length(brand)<1 then '扬子江' else brand end as brand_clean,\n" +
                    "num,\n" +
                    "price,\n" +
                    "properties_name,\n" +
                    "quantity,\n" +
                    "sku_id,\n" +
                    "规格,\n" +
                    "包装,\n" +
                    "brand2,\n" +
                    "CASE \n" +
                    "  WHEN 规格2 IN (1, 24, 18, 10, 100, 9) THEN\n" +
                    "    COALESCE(\n" +
                    "      -- 当title包含\"散风通窍\"时，只匹配以\"丸\"为单位的数字\n" +
                    "      NULLIF(\n" +
                    "        TRIM(\n" +
                    "          CASE \n" +
                    "            WHEN title LIKE '%散风通窍%' THEN\n" +
                    "              REGEXP_SUBSTR(title, '\\\\d+丸')\n" +
                    "            ELSE\n" +
                    "              -- 从properties_name提取\n" +
                    "              NULLIF(\n" +
                    "                TRIM(\n" +
                    "                  CASE\n" +
                    "                    -- 匹配x前的数字和单位\n" +
                    "                    WHEN properties_name REGEXP '.*x.*' THEN\n" +
                    "                      REGEXP_SUBSTR(properties_name, '\\\\d+(支|丸|袋|盒|粒|片|瓶)\\\\s')\n" +
                    "                    -- 匹配*后的数字和单位\n" +
                    "                    WHEN properties_name REGEXP '.*\\\\*.*' THEN\n" +
                    "                      REGEXP_SUBSTR(properties_name, '\\\\*\\\\s*\\\\d+(支|丸|袋|盒|粒|片|瓶)')\n" +
                    "                  END\n" +
                    "                ), ''\n" +
                    "              )\n" +
                    "          END\n" +
                    "        ), ''\n" +
                    "      ),\n" +
                    "      -- 如果上述都为空，尝试从title提取\n" +
                    "      NULLIF(\n" +
                    "        TRIM(\n" +
                    "          CASE \n" +
                    "            WHEN title LIKE '%苏黄%' AND NOT title LIKE '%散风通窍%' THEN\n" +
                    "              REGEXP_SUBSTR(title, '\\\\d+粒')\n" +
                    "            ELSE\n" +
                    "              -- 除了特殊情况外，匹配所有单位的数字\n" +
                    "              IF(title NOT LIKE '%散风通窍%', REGEXP_SUBSTR(title, '\\\\d+(支|袋|盒|粒|片|瓶|丸)'), NULL)\n" +
                    "          END\n" +
                    "        ), ''\n" +
                    "      ),\n" +
                    "      -- 如果上述都为空，则返回原规格2值\n" +
                    "      规格2\n" +
                    "    )\n" +
                    "ELSE 规格2 -- 或其他默认处理\n" +
                    "END AS 规格2,\n" +
                    "  单盒价,\n" +
                    "case  \n" +
                    "WHEN title LIKE '%蓝芩口服液%' AND 规格2 IN ('7', '9', '10','12') THEN '店铺不合格'\n" +
                    "WHEN title LIKE '%黄芪精%' AND 规格2 IN ('60', '30', '24','18') THEN '店铺不合格'\n" +
                    "WHEN title LIKE '%可乐可康%' AND 规格2 IN ('12', '18', '36')  THEN '店铺不合格'\n" +
                    "WHEN title LIKE '%星迪%' AND 规格2 IN ('28', '21')  THEN '店铺不合格'\n" +
                    "WHEN title LIKE '%贝雪%' AND 规格2 IN ('5')  THEN '店铺不合格'\n" +
                    "WHEN title LIKE '%补肾润肺%' AND 规格2 IN ('3')  THEN '店铺不合格'\n" +
                    "WHEN title LIKE '%苏黄%' AND 规格2 IN ('9')  THEN '店铺不合格'\n" +
                    "WHEN (title LIKE '%依帕司他片（唐林）%' OR title LIKE '%唐林胶囊%') AND 规格2 IN ('18')  THEN '店铺不合格'\n" +
                    "WHEN title LIKE '%散风通窍滴丸%' and 规格2 IN ('60')  THEN '店铺不合格'\n" +
                    "WHEN title LIKE '%杏荷止咳%' and 规格2 IN ('150')  THEN '店铺不合格'\n" +
                    "\n" +
                    "\n" +
                    "WHEN title LIKE '%蓝芩口服液%' AND 规格2 like '%6支%'  and 单盒价 < 31.5 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%蓝芩口服液%' AND 规格2 like '%12%' and 单盒价 < 63.50 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%蓝芩口服液%' AND 规格2 like '%14%' and 单盒价 < 74.09 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%蓝芩口服液%' AND 规格2 = 1 and 单盒价 <  31.5 and quantity <>0 THEN '不合格'\n" +
                    "\n" +
                    "WHEN title LIKE '%黄芪精%' AND 规格2 = '%6%' and 单盒价 < 22.5 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%黄芪精%' AND 规格2 = '%12%' and 单盒价 < 40.5 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%黄芪精%' AND 规格2 = 1 and 单盒价 < 22.5 and quantity <>0 THEN '不合格'\n" +
                    "\n" +
                    "WHEN title LIKE '%可乐可康%' AND 规格2 = '%30%' and 单盒价 < 81 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%可乐可康%' AND 规格2 = 1 and 单盒价 < 72 and quantity <>0 THEN '不合格'\n" +
                    "\n" +
                    "WHEN title LIKE '%香芍颗粒%' AND 规格2 like '%4g×3袋%' and 单盒价 < 50.4 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%香芍颗粒%' AND 规格2 like '%4g×9袋%' and 单盒价 < 135 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%香芍颗粒%' AND 规格2 like '%4g×12袋%' and 单盒价 < 201.6 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%香芍颗粒%' AND 规格2 =1 and 单盒价 < 50.4 and quantity <>0 THEN '不合格'\n" +
                    "\n" +
                    "WHEN title LIKE '%神曲消食%' AND 规格2 like '%6%' and 单盒价 < 64.8 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%神曲消食%' AND 规格2 = 1 and 单盒价 < 64.8 and quantity <>0 THEN '不合格'\n" +
                    "\n" +
                    "WHEN title LIKE '%贝雪%' AND 规格2 like '%6%' and 单盒价 < 55.13 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%贝雪%' AND 规格2 like '%3%' and 单盒价 < 28.8 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%贝雪%' AND 规格2 = 1 and 单盒价 < 28.8 and quantity <>0 THEN '不合格'\n" +
                    "\n" +
                    "\n" +
                    "WHEN title LIKE '%百安新%' AND 规格2 like '%12.5mg×7%' and 单盒价 < 37.35 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%百安新%' AND 规格2 =1 and 单盒价 < 37.35 and quantity <>0 THEN '不合格'\n" +
                    "\n" +
                    "WHEN title LIKE '%依帕司他片（唐林）%' AND 规格2 = '%10%' and 单盒价 < 30.96 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%依帕司他片（唐林）%' AND 规格2 = 1 and 单盒价 < 30.96 and quantity <>0 THEN '不合格'\n" +
                    "\n" +
                    "WHEN title LIKE '%唐林胶囊%' AND 规格2 like 10 and 单盒价 < 33.34 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%唐林胶囊%' AND 规格2 = 1 and 单盒价 < 33.34 and quantity <>0 THEN '不合格'\n" +
                    "\n" +
                    "WHEN title LIKE '%荜铃胃痛%' AND 规格2 like 6 and 单盒价 < 27.96 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%荜铃胃痛%' AND 规格2 = 1 and 单盒价 < 27.96 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%荜铃胃痛%' AND 规格2 like 9 and 单盒价 < 41.94 and quantity <>0 THEN '不合格'\n" +
                    "\n" +
                    "WHEN title LIKE '%散风通窍%' AND 规格2 like 100 and 单盒价 < 77.4 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%散风通窍%' AND 规格2 = 1 and 单盒价 < 77.4 and quantity <>0 THEN '不合格'\n" +
                    "\n" +
                    "WHEN title LIKE '%卢苏%' AND 规格2 like 3 and 单盒价 < 48.61 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%卢苏%' AND 规格2 = 1 and 单盒价 < 48.61 and quantity <>0 THEN '不合格'\n" +
                    "WHEN title LIKE '%卢苏%' AND 规格2 like 5 and 单盒价 < 81.02 and quantity <>0 THEN '不合格'\n" +
                    "else '合格'\n" +
                    "end as if_reg,\n" +
                    "snap_url\n" +
                    "from tm_item_final tif )a\n" +
                    "left join company_local_business1 clb \n" +
                    "on a.平台 = clb.平台 \n" +
                    "and a.nick = clb.店铺名称 \n" +
                    "where title not like '%其他品牌_%'\n" +
                    "and title <> '星迪' and 规格2 <> '14片'\n" +
                    "and quantity <> 0\n" +
                    "and if_reg <> '合格'\n" +
                    "and brand_clean <> '福人'\n" +
                    ";"; // 填入您的完整SQL
            PreparedStatement withStmt = conn.prepareStatement(withQuery);
            ResultSet rs = withStmt.executeQuery();

            // 准备插入语句
            PreparedStatement insertStmt = conn.prepareStatement(insertSQL);

            while (rs.next()) {
                // 假设列id, num_iid等是查询结果的列名
                insertStmt.setString(1, rs.getString( "平台"));
                insertStmt.setString(2, rs.getString( "id"));
                insertStmt.setString(3, rs.getString( "num_iid"));
                insertStmt.setString(4, rs.getString( "title"));
                insertStmt.setString(5, rs.getString( "name"));
                insertStmt.setString(6, rs.getString( "nick"));
                insertStmt.setString(7, rs.getString( "detail_url"));
                insertStmt.setString(8, rs.getString( "pic_url"));
                insertStmt.setString(9, rs.getString( "brand_clean"));
                insertStmt.setString(10, rs.getString("num"));
                insertStmt.setString(11, rs.getString("price"));
                insertStmt.setString(12, rs.getString("单盒价"));
                insertStmt.setString(13, rs.getString("properties_name"));
                insertStmt.setString(14, rs.getString("quantity"));
                insertStmt.setString(15, rs.getString("sku_id"));
                insertStmt.setString(16, rs.getString("规格"));
                insertStmt.setString(17, rs.getString("包装"));
                insertStmt.setString(18, rs.getString("brand2"));
                insertStmt.setString(19, rs.getString("规格2"));
                insertStmt.setString(20, rs.getString("if_reg"));
                insertStmt.setString(21, rs.getString("snap_url"));
                insertStmt.setString(22, rs.getString("省区"));
                insertStmt.setString(23, rs.getString("城市"));
                insertStmt.setString(24, rs.getString("商业公司"));
                insertStmt.executeUpdate();
            }

            System.out.println("Data inserted successfully.");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
