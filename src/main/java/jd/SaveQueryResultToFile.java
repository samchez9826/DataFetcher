package jd;

import java.sql.*;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;

public class SaveQueryResultToFile {
    public static void main(String[] args) {
        // 数据库连接参数
        String url = "jdbc:mysql://rm-uf6n4777qk62x4p60vo.rwlb.rds.aliyuncs.com:3306/yang_test?useSSL=false&serverTimezone=UTC";
        String user = "wzy";
        String password = "wuzhiyuan421524YUAO+";

        // SQL查询
        String query = "SET @row_num := 0;\n" +
                "SET @current_platform := '';\n" +
                "SET @current_province := '';\n" +
                "SET @current_city := '';\n" +
                "SET @current_title := '';\n" +
                "SET @current_nick := '';\n" +
                "SET @current_spec := '';\n" +
                "SET @current_package := '';\n" +
                "SET @current_company := '';\n" +
                " -- 假设 t2 有 row_num 字段\n" +
                "\n" +
                "with tmp1 as (select * from (SELECT\n" +
                "    @row_num := IF(@current_platform = `平台` AND @current_province = `省区` AND @current_city = `城市` AND @current_title = `title` AND @current_nick = `nick` AND @current_spec = `规格2` AND @current_package = `包装` AND @current_company = `商业公司`, @row_num + 1, 1) AS row_num,\n" +
                "    @current_platform := `平台`,\n" +
                "    @current_province := `省区`,\n" +
                "    @current_city := `城市`,\n" +
                "    @current_title := `title`,\n" +
                "    @current_nick := `nick`,\n" +
                "    @current_spec := `规格2`,\n" +
                "    @current_package := `包装`,\n" +
                "    @current_company := `商业公司`,\n" +
                "    tt.*\n" +
                "FROM\n" +
                "    (SELECT * FROM tt1 ORDER BY `平台`, `省区`, `城市`, `title`, `nick`, `规格2`, `包装`, `商业公司`) AS tt)a\n" +
                "where row_num = 1)\n" +
                ",t2 AS (\n" +
                "    SELECT @row_num := @row_num + 1 AS row_num, ih.*\n" +
                "    FROM item_history ih\n" +
                "    WHERE 申诉状态 NOT LIKE '%新建%'\n" +
                "    ORDER BY 1 -- Replace [SomeColumn] with the column name you want to sort by\n" +
                ")\n" +
                ",com as (select \n" +
                "t2.id as row_num,\n" +
                "t1.平台,\n" +
                "t2.组,\n" +
                "t2.区域公司,\n" +
                "t1.省区,\n" +
                "t1.城市,\n" +
                "t1.title as 产品,\n" +
                "t1.规格2 as 产品规格,\n" +
                "t1.店铺名称,\n" +
                "t1.商业公司,\n" +
                "t1.detail_url as `pc链接(可点击)`,\n" +
                "t1.id as 商品id,\n" +
                "REGEXP_REPLACE(t1.包装, '\\\\D', '') as 盒数,\n" +
                "t2.`2月26日单盒价`,\n" +
                "t2.`2月28日单盒价`,\n" +
                "t2.`3月1日单盒价`,\n" +
                "t2.`3月4日单盒价`,\n" +
                "t1.单盒价 as `3月11日单盒价`,\n" +
                "t2.`2月26日页面价`,\n" +
                "t2.`2月28日页面价`,\n" +
                "t2.`3月1日页面价`,\n" +
                "t2.`3月4日页面价`,\n" +
                "t1.price as `3月11日页面价`,\n" +
                "t2.`2月26日商品状态`,\n" +
                "t2.`是否合格2月26日`,\n" +
                "t2.`是否考核2月26日`,\n" +
                "t2.`2月28日商品状态`,\n" +
                "t2.`是否合格2月28日`,\n" +
                "t2.`是否考核2月28日`,\n" +
                "t2.`3月1日商品状态`,\n" +
                "t2.`是否合格3月1日`,\n" +
                "t2.`是否考核3月1日`,\n" +
                "t2.`3月4日商品状态`,\n" +
                "t2.`是否合格3月4日`,\n" +
                "t2.`是否考核3月4日`,\n" +
                "case when t2.平台 is null then '新增' else '在售' end as `3月11日商品状态`,\n" +
                "t1.if_reg as `是否合格3月11日`,\n" +
                "null as `是否考核3月11日`,\n" +
                "t2.申诉状态,\n" +
                "t2.终端\n" +
                "from tmp1 t1\n" +
                "left join\n" +
                " t2\n" +
                "on\n" +
                "t1.平台 =t2.平台\n" +
                "and\n" +
                "t1.省区 =t2.省区\n" +
                "and\n" +
                "t1.城市 =t2.城市\n" +
                "and\n" +
                "t1.title =t2.产品\n" +
                "and\n" +
                "t1.店铺名称 =t2.店铺名称\n" +
                "and\n" +
                "t1.规格2 =t2.产品规格\n" +
                "and\n" +
                "t1.包装 = concat(t2.盒数,\"盒\")\n" +
                "and\n" +
                "t1.商业公司 =t2.商业公司\n" +
                "union\n" +
                "\n" +
                "SELECT\n" +
                "t2.id,\n" +
                "t2.平台,\n" +
                "t2.组,\n" +
                "t2.区域公司,\n" +
                "t2.省区,\n" +
                "t2.城市,\n" +
                "t2.产品,\n" +
                "t2.产品规格,\n" +
                "t2.店铺名称,\n" +
                "t2.商业公司,\n" +
                "t2.pc链接 as `pc链接(可点击)`,\n" +
                "t2.商品id,\n" +
                "case when t2.盒数 like '%null%' then '/' else t2.盒数 end as `盒数`,\n" +
                "case when 2月26日单盒价 is null then '/' else 2月26日单盒价 end as 2月26日单盒价,\n" +
                "case when 2月28日单盒价 is null then '/' else 2月28日单盒价 end as 2月28日单盒价,\n" +
                "case when 3月1日单盒价 is null then '/' else 3月1日单盒价 end as 3月1日单盒价,\n" +
                "case when 3月4日单盒价 is null then '/' else 3月4日单盒价 end as 3月4日单盒价,\n" +
                "'/' as `3月11日单盒价`,\n" +
                "case when t2.`2月26日页面价` is null then '/' else t2.`2月26日页面价` end as `2月26日页面价`,\n" +
                "case when t2.`2月28日页面价` is null then '/' else t2.`2月28日页面价` end as `2月28日页面价`,\n" +
                "case when t2.`3月1日页面价` is null then '/' else t2.`3月1日页面价`  end as `3月1日页面价` ,\n" +
                "case when t2.`3月4日页面价` is null then '/' else t2.`3月4日页面价`  end as `3月4日页面价` ,\n" +
                "'/' as `3月11日页面价`,\n" +
                "case when t2.`2月26日商品状态` is null then '/' else `2月26日商品状态` end as `2月26日商品状态`,\n" +
                "case when t2.`是否合格2月26日` is null then '/' else `是否合格2月26日` end as `是否合格2月26日`,\n" +
                "case when t2.`是否考核2月26日` is null then '/' else `是否考核2月26日` end as `是否考核2月26日`,\n" +
                "case when t2.`2月28日商品状态` is null then '/' else `2月28日商品状态` end as `2月28日商品状态`,\n" +
                "case when t2.`是否合格2月28日` is null then '/' else `是否合格2月28日` end as `是否合格2月28日`,\n" +
                "case when t2.`是否考核2月28日` is null then '/' else `是否考核2月28日` end as `是否考核2月28日`,\n" +
                "case when t2.`3月1日商品状态` is null then '/' else `3月1日商品状态` end as `3月1日商品状态`,\n" +
                "case when t2.`是否合格3月1日` is null then '/' else `是否合格3月1日` end as `是否合格3月1日`,\n" +
                "case when t2.`是否考核3月1日` is null then '/' else `是否考核3月1日` end as `是否考核3月1日`,\n" +
                "case when t2.`3月4日商品状态` is null then '/' else `3月4日商品状态` end as `3月4日商品状态`,\n" +
                "case when t2.`是否合格3月4日` is null then '/' else `是否合格3月4日` end as `是否合格3月4日`,\n" +
                "case when t2.`是否考核3月4日` is null then '/' else `是否考核3月4日` end as `是否考核3月4日`,\n" +
                "'下架' as `3月11日商品状态`,\n" +
                "'合格' as `是否合格3月11日`,\n" +
                "null as `是否考核3月11日`,\n" +
                "t2.申诉状态,\n" +
                "t2.终端\n" +
                "from tmp1 t1\n" +
                "right join\n" +
                "t2\n" +
                "on\n" +
                "t1.平台 =t2.平台\n" +
                "and\n" +
                "t1.省区 =t2.省区\n" +
                "and\n" +
                "t1.城市 =t2.城市\n" +
                "and\n" +
                "t1.title =t2.产品\n" +
                "and\n" +
                "t1.店铺名称 =t2.店铺名称\n" +
                "and\n" +
                "t1.规格2 =t2.产品规格\n" +
                "and\n" +
                "t1.包装 = concat(t2.盒数,\"盒\")\n" +
                "and\n" +
                "t1.商业公司 =t2.商业公司)\n" +
                "\n" +
                "select * from (SELECT\n" +
                "    @row_num := @row_num + 1 AS new_row_num,\n" +
                "case when row_num is null then @row_num := @row_num + 1 else row_num end as rr1,\n" +
                "平台,\n" +
                "组,\n" +
                "区域公司,\n" +
                "省区,\n" +
                "城市,\n" +
                "产品,\n" +
                "产品规格,\n" +
                "店铺名称,\n" +
                "商业公司,\n" +
                " `pc链接(可点击)`,\n" +
                "商品id,\n" +
                "case when 盒数 = 'null' then '/' else 盒数 end as 盒数,\n" +
                "case when 2月26日单盒价 = 'null' then '/' when 2月26日单盒价 is null then '/' else 2月26日单盒价 end as 2月26日单盒价,\n" +
                "case when 2月28日单盒价 = 'null' then '/' when 2月28日单盒价 is null then '/' else 2月28日单盒价 end as 2月28日单盒价,\n" +
                "case when 3月1日单盒价 = 'null' then '/' when 3月1日单盒价 is null then '/' else 3月1日单盒价 end as 3月1日单盒价,\n" +
                "case when 3月4日单盒价 = 'null' then '/' when 3月4日单盒价 is null then '/' else 3月4日单盒价 end as 3月4日单盒价,\n" +
                "case when 3月11日单盒价 = 'null' then '/' else 3月11日单盒价 end as 3月11日单盒价,\n" +
                "case when 2月26日页面价 = 'null' then '/' when 2月26日页面价 is null then '/' else 2月26日页面价 end as 2月26日页面价,\n" +
                "case when 2月28日页面价 = 'null' then '/' when 2月28日页面价 is null then '/' else 2月28日页面价 end as 2月28日页面价,\n" +
                "case when 3月1日页面价 = 'null' then '/' when 3月1日页面价 is null then '/' else 3月1日页面价 end as 3月1日页面价,\n" +
                "case when 3月4日页面价 = 'null' then '/' when 3月4日页面价 is null then '/' else 3月4日页面价 end as 3月4日页面价,\n" +
                "case when 3月11日页面价 = 'null' then '/' else 3月11日页面价 end as 3月11日页面价,\n" +
                "case when 2月26日商品状态 = 'null' then '/' when 2月26日商品状态 is null then '/' else 2月26日商品状态 end as 2月26日商品状态,\n" +
                "case when 是否合格2月26日 = 'null' then '/' when 是否合格2月26日 is null then '/' else 是否合格2月26日 end as 是否合格2月26日,\n" +
                "是否考核2月26日,\n" +
                "case when 2月28日商品状态 = 'null' then '/' when 2月28日商品状态 is null then '/' else 2月28日商品状态 end as 2月28日商品状态,\n" +
                "case when 是否合格2月28日 = 'null' then '/' when 是否合格2月28日 is null then '/' else 是否合格2月28日 end as 是否合格2月28日,\n" +
                "是否考核2月28日,\n" +
                "case when 3月1日商品状态 = 'null' then '/' when 3月1日商品状态 is null then '/' else 3月1日商品状态 end as 3月1日商品状态,\n" +
                "case when 是否合格3月1日 = 'null' then '/' when 是否合格3月1日 is null then '/' else 是否合格3月1日 end as 是否合格3月1日,\n" +
                "case when 是否考核3月1日 = 'null' then '/' when 是否考核3月1日 is null then '/' else 是否考核3月1日 end as 是否考核3月1日,\n" +
                "case when 3月4日商品状态 = 'null' then '/' when 3月4日商品状态 is null then '/' else 3月4日商品状态 end as 3月4日商品状态,\n" +
                "case when 是否合格3月4日 = 'null' then '/' when 是否合格3月4日 is null then '/' else 是否合格3月4日 end as 是否合格3月4日,\n" +
                "是否考核3月4日,\n" +
                "3月11日商品状态,\n" +
                "是否合格3月11日,\n" +
                "是否考核3月11日,\n" +
                "申诉状态,\n" +
                "终端\n" +
                "\n" +
                "FROM\n" +
                "    com\n" +
                "ORDER BY\n" +
                "com.row_num)a order by rr1\n" +
                ";'";

        // 输出文件路径
        String filePath = "query_results.txt";

        try (Connection conn = DriverManager.getConnection(url, user, password);
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery();
             FileWriter fileWriter = new FileWriter(filePath);
             PrintWriter printWriter = new PrintWriter(fileWriter)) {

            // 遍历查询结果集并写入文件
            while (rs.next()) {
                String detailUrl = rs.getString("detail_url");
                printWriter.println(detailUrl);
            }

            System.out.println("Data has been written to " + filePath);

        } catch (SQLException e) {
            System.out.println("Database access error:");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("File writing error:");
            e.printStackTrace();
        }
    }
}
