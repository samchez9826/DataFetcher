import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashSet;
import java.util.Set;

public class FinalProcessor {
    public static void main(String[] args) {
        String url = "jdbc:mysql://your_database_url/your_database_name?useSSL=false&serverTimezone=UTC";
        String username = "your_username";
        String password = "your_password";

        try (Connection conn = DriverManager.getConnection(url, username, password)) {
            // 1. 查询所有独特的检查日期
            Set<LocalDate> dates = new LinkedHashSet<>();
            String dateQuery = "SELECT DISTINCT 检查时间 FROM original_table_name";
            try (Statement dateStmt = conn.createStatement();
                 ResultSet dateRs = dateStmt.executeQuery(dateQuery)) {
                while (dateRs.next()) {
                    String checkTimeStr = dateRs.getString("检查时间");
                    LocalDate checkTime;
                    if (checkTimeStr.contains(" ")) {
                        checkTime = LocalDate.parse(checkTimeStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    } else {
                        checkTime = LocalDate.parse(checkTimeStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    }
                    dates.add(checkTime);
                }
            }

            // 2. 动态地创建一个新表
            String newTableName = "new_dynamic_table";
            StringBuilder createTableSql = new StringBuilder("CREATE TABLE IF NOT EXISTS " + newTableName + " (id INT AUTO_INCREMENT PRIMARY KEY, 平台 VARCHAR(255), 组 VARCHAR(255), 区域公司 VARCHAR(255), 省区 VARCHAR(255), 城市 VARCHAR(255), 产品 VARCHAR(255), 产品规格 VARCHAR(255), 店铺名称 VARCHAR(255), 商业公司 VARCHAR(255), 商品标题 TEXT, pc链接（可点击） TEXT, 商品id VARCHAR(255), 盒数 INT, 销量 INT, 预计销售总额 DECIMAL(10,2), 发货地址 TEXT, 地址 TEXT, 申诉状态 VARCHAR(255), 终端 VARCHAR(255)");
            dates.forEach(date -> {
                String formattedDate = date.format(DateTimeFormatter.ofPattern("M月d日"));
                createTableSql.append(", `").append(formattedDate).append("单盒价` DECIMAL(10,2)")
                        .append(", `").append(formattedDate).append("页面价` DECIMAL(10,2)")
                        .append(", `").append(formattedDate).append("商品状态` VARCHAR(255)")
                        .append(", `是否合格").append(formattedDate).append("` VARCHAR(255)")
                        .append(", `是否考核").append(formattedDate).append("` VARCHAR(255)");
            });
            createTableSql.append(")");

            try (Statement createTableStmt = conn.createStatement()) {
                createTableStmt.executeUpdate(createTableSql.toString());
            }

            // 3. 插入数据到新表中
            // 注意：这里需要添加实际的插入逻辑，包括准备数据和执行插入操作
            // 由于插入逻辑依赖于具体的数据和业务需求，这部分代码留给你根据实际情况来实现

            conn.commit(); // 提交事务
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
