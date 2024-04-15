package tm;

import java.sql.*;

public class HistoryCombindNewD {

    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3307/retail?useSSL=false&serverTimezone=UTC&characterEncoding=UTF-8";
        String user = "root";
        String password = "root1234";

        // 假设要插入的表结构和tmp4查询结果一致
        String insertSQL = "INSERT INTO retail.f2(row_num, 平台, 组, 区域公司, 省区, 城市, 产品, 产品规格, 店铺名称, 商业公司, `pc链接(可点击)`, 商品id, 盒数, `2月26日单盒价`, `2月28日单盒价`, `3月1日单盒价`, `3月4日单盒价`, `3月22日单盒价`, `2月26日页面价`, `2月28日页面价`, `3月1日页面价`, `3月4日页面价`, `3月22日页面价`, `2月26日商品状态`, 是否合格2月26日, 是否考核2月26日, `2月28日商品状态`, 是否合格2月28日, 是否考核2月28日, `3月1日商品状态`, 是否合格3月1日, 是否考核3月1日, `3月4日商品状态`, 是否合格3月4日, 是否考核3月4日, `3月22日商品状态`, 是否合格3月22日, 是否考核3月22日, 申诉状态, 终端) VALUES (?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {
            // 单独执行每一个 SET 语句
            String[] initSqls = {
                    "SET @max_row_num := (SELECT MAX(row_num) FROM retail.f2);",
                    "SET @row_num := 0",
                    "SET @current_platform := ''",
                    "SET @current_province := ''",
                    "SET @current_city := ''",
                    "SET @current_title := ''",
                    "SET @current_nick := ''",
                    "SET @current_spec := ''",
                    "SET @current_package := ''",
                    "SET @current_company := ''"
            };
            for (String sql : initSqls) {
                stmt.execute(sql);
            }

            // 执行WITH查询并获取结果
            String withQuery =
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
                    "    (SELECT * FROM tm_new_add_detial ORDER BY `平台`, `省区`, `城市`, `title`, `nick`, `规格2`, `包装`, `商业公司`) AS tt)a\n" +
                    "where row_num = 1)\n" +
                    ",t2 AS (\n" +
                    "    SELECT *\n" +
                    "    FROM item_history ih\n" +
                    "    WHERE 申诉状态 NOT LIKE '%新建%'\n" +
                    "    ORDER BY id -- Replace [SomeColumn] with the column name you want to sort by\n" +
                    ")\n" +
                    ",com as (select \n" +
                    "t2.id as row_num ,\n" +
                    "t1.平台,\n" +
                    "t2.组,\n" +
                    "t2.区域公司,\n" +
                    "t1.省区,\n" +
                    "t1.城市,\n" +
                    "t1.title as 产品,\n" +
                    "t1.规格2 as 产品规格,\n" +
                    "t1.nick as 店铺名称,\n" +
                    "t1.商业公司,\n" +
                    "t1.detail_url as `pc链接(可点击)`,\n" +
                    "t1.id as 商品id,\n" +
                    "REGEXP_REPLACE(t1.包装, '\\\\D', '') as 盒数,\n" +
                    "t2.`2月26日单盒价`,\n" +
                    "t2.`2月28日单盒价`,\n" +
                    "t2.`3月1日单盒价`,\n" +
                    "t2.`3月4日单盒价`,\n" +
                    "t1.单盒价 as `3月22日单盒价`,\n" +
                    "t2.`2月26日页面价`,\n" +
                    "t2.`2月28日页面价`,\n" +
                    "t2.`3月1日页面价`,\n" +
                    "t2.`3月4日页面价`,\n" +
                    "t1.price as `3月22日页面价`,\n" +
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
                    "case when t2.平台 is null then '新增' else '在售' end as `3月22日商品状态`,\n" +
                    "t1.if_reg as `是否合格3月22日`,\n" +
                    "null as `是否考核3月22日`,\n" +
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
                    "t1.nick =t2.店铺名称\n" +
                    "and\n" +
                    "t1.规格2 =t2.产品规格\n" +
                    "and\n" +
                    "t1.包装 = concat(t2.盒数,\"盒\")\n" +
                    "and\n" +
                    "t1.商业公司 =t2.商业公司\n" +
                    "union\n" +
                    "\n" +
                    "SELECT\n" +
                    "t2.id as row_num ,\n" +
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
                    "'/' as `3月22日单盒价`,\n" +
                    "case when t2.`2月26日页面价` is null then '/' else t2.`2月26日页面价` end as `2月26日页面价`,\n" +
                    "case when t2.`2月28日页面价` is null then '/' else t2.`2月28日页面价` end as `2月28日页面价`,\n" +
                    "case when t2.`3月1日页面价` is null then '/' else t2.`3月1日页面价`  end as `3月1日页面价` ,\n" +
                    "case when t2.`3月4日页面价` is null then '/' else t2.`3月4日页面价`  end as `3月4日页面价` ,\n" +
                    "'/' as `3月22日页面价`,\n" +
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
                    "'下架' as `3月22日商品状态`,\n" +
                    "'合格' as `是否合格3月22日`,\n" +
                    "null as `是否考核3月22日`,\n" +
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
                    "t1.nick =t2.店铺名称\n" +
                    "and\n" +
                    "t1.规格2 =t2.产品规格\n" +
                    "and\n" +
                    "t1.包装 = concat(t2.盒数,\"盒\")\n" +
                    "and\n" +
                    "t1.商业公司 =t2.商业公司)\n" +
                    "\n" +
                    "SELECT\n" +
                    "row_num,\n" +
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
                    "case when 3月22日单盒价 = 'null' then '/' else 3月22日单盒价 end as 3月22日单盒价,\n" +
                    "case when 2月26日页面价 = 'null' then '/' when 2月26日页面价 is null then '/' else 2月26日页面价 end as 2月26日页面价,\n" +
                    "case when 2月28日页面价 = 'null' then '/' when 2月28日页面价 is null then '/' else 2月28日页面价 end as 2月28日页面价,\n" +
                    "case when 3月1日页面价 = 'null' then '/' when 3月1日页面价 is null then '/' else 3月1日页面价 end as 3月1日页面价,\n" +
                    "case when 3月4日页面价 = 'null' then '/' when 3月4日页面价 is null then '/' else 3月4日页面价 end as 3月4日页面价,\n" +
                    "case when 3月22日页面价 = 'null' then '/' else 3月22日页面价 end as 3月22日页面价,\n" +
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
                    "3月22日商品状态,\n" +
                    "是否合格3月22日,\n" +
                    "是否考核3月22日,\n" +
                    "申诉状态,\n" +
                    "终端\n" +
                    "\n" +
                    "FROM\n" +
                    "    com\n" +
                    "ORDER BY\n" +
                    "    com.row_num ;"; // 填入您的完整SQL
            PreparedStatement withStmt = conn.prepareStatement(withQuery);
            ResultSet rs = withStmt.executeQuery();

            // 准备插入语句
            PreparedStatement insertStmt = conn.prepareStatement(insertSQL);

            while (rs.next()) {
                insertStmt.setInt(1, rs.getInt( "row_num"));
                insertStmt.setString(2, rs.getString( "平台"));
                insertStmt.setString(3, rs.getString( "组"));
                insertStmt.setString(4, rs.getString( "区域公司"));
                insertStmt.setString(5, rs.getString( "省区"));
                insertStmt.setString(6, rs.getString( "城市"));
                insertStmt.setString(7, rs.getString( "产品"));
                insertStmt.setString(8, rs.getString( "产品规格"));
                insertStmt.setString(9, rs.getString( "店铺名称"));
                insertStmt.setString(10, rs.getString("商业公司"));
                insertStmt.setString(11, rs.getString("pc链接(可点击)"));
                insertStmt.setString(12, rs.getString("商品id"));
                insertStmt.setString(13, rs.getString("盒数"));
                insertStmt.setString(14, rs.getString("2月26日单盒价"));
                insertStmt.setString(15, rs.getString("2月28日单盒价"));
                insertStmt.setString(16, rs.getString("3月1日单盒价"));
                insertStmt.setString(17, rs.getString("3月4日单盒价"));
                insertStmt.setString(18, rs.getString("3月22日单盒价"));
                insertStmt.setString(19, rs.getString("2月26日页面价"));
                insertStmt.setString(20, rs.getString("2月28日页面价"));
                insertStmt.setString(21, rs.getString("3月1日页面价"));
                insertStmt.setString(22, rs.getString("3月4日页面价"));
                insertStmt.setString(23, rs.getString("3月22日页面价"));
                insertStmt.setString(24, rs.getString("2月26日商品状态"));
                insertStmt.setString(25, rs.getString("是否合格2月26日"));
                insertStmt.setString(26, rs.getString("是否考核2月26日"));
                insertStmt.setString(27, rs.getString("2月28日商品状态"));
                insertStmt.setString(28, rs.getString("是否合格2月28日"));
                insertStmt.setString(29, rs.getString("是否考核2月28日"));
                insertStmt.setString(30, rs.getString("3月1日商品状态"));
                insertStmt.setString(31, rs.getString("是否合格3月1日"));
                insertStmt.setString(32, rs.getString("是否考核3月1日"));
                insertStmt.setString(33, rs.getString("3月4日商品状态"));
                insertStmt.setString(34, rs.getString("是否合格3月4日"));
                insertStmt.setString(35, rs.getString("是否考核3月4日"));
                insertStmt.setString(36, rs.getString("3月22日商品状态"));
                insertStmt.setString(37, rs.getString("是否合格3月22日"));
                insertStmt.setString(38, rs.getString("是否考核3月22日"));
                insertStmt.setString(39, rs.getString("申诉状态"));
                insertStmt.setString(40, rs.getString("终端"));
                insertStmt.executeUpdate();
            }

            System.out.println("Data inserted successfully.");
            // 执行更新操作，设置新的 ID
            stmt.execute("SET @max_id = (SELECT MAX(id) FROM retail.f2);"); // 获取当前最大ID

            // 准备更新语句
            String updateSQL = "UPDATE retail.f2 SET id = (@max_id := @max_id + 1) WHERE row_num = 0 ORDER BY row_num;";
            // 注意：这里的ORDER BY实际上可能不起作用，因为所有row_num都是0。这个ORDER BY在这个语句中不影响变量的递增。

            // 执行更新语句
            stmt.execute(updateSQL);

            System.out.println("Data inserted and IDs updated successfully.");


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
