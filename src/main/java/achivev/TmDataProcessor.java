package achivev;

import org.json.JSONObject;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;


public class TmDataProcessor {


    private static Map<String, Boolean> imageColorTypeCache = new HashMap<>();

    public static void main(String[] args) {
        try {
            Connection conn = DriverManager.getConnection("jdbc:mysql://rm-uf6n4777qk62x4p60vo.rwlb.rds.aliyuncs.com:3306/yang_test?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC"
                    , "wzy"
                    , "wuzhiyuan421524YUAO+");
            Statement stmt = conn.createStatement();
            String sql = "SELECT id, num_iid, title, nick,detail_url, CASE WHEN pic_url NOT LIKE 'https:%' THEN CONCAT('https:', pic_url) ELSE pic_url END AS pic_url, CASE WHEN brand IS NULL AND title LIKE '%扬子江%' THEN '扬子江' ELSE brand END AS brand, num, price, properties_name, quantity, sku_id, sku_url, CONCAT('https://detail.tmall.com/item.htm?id=', num_iid, '&skuId=', sku_id) AS url_f FROM tm_item_sku_combined tisc";
            ResultSet rs = stmt.executeQuery(sql);


            while (rs.next()) {
                long id = rs.getLong("id");
                long num_iid = rs.getLong("num_iid");
                String title = rs.getString("title");
                String nick = rs.getString("nick");
                String detail_url = rs.getString("detail_url");
                String pic_url = rs.getString("pic_url");
                String brand = rs.getString("brand");
                int num = rs.getInt("num");
                double price = rs.getDouble("price");
                String properties_name = rs.getString("properties_name");
                int quantity = rs.getInt("quantity");
                long sku_id = rs.getLong("sku_id");
                String url_f = rs.getString("url_f"); // 获取新增的字段

                if (isImageBlue(pic_url)) {
                    JSONObject record = new JSONObject();
                    record.put("id", id);
                    record.put("num_iid", num_iid);
                    record.put("title", title);
                    record.put("nick", nick);
                    record.put("detail_url", detail_url);
                    record.put("pic_url", pic_url);
                    record.put("brand", brand);
                    record.put("num", num);
                    record.put("price", price);
                    record.put("properties_name", properties_name);
                    record.put("quantity", quantity);
                    record.put("sku_id", sku_id);
                    record.put("url_f", url_f); // 将新字段添加到 JSONObject

                    applyRulesBasedOnProductTitle(record, conn);
                    saveCleanedData(conn, record);
                }
            }
            rs.close();
            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }




    private static boolean isImageBlue(String imageUrl) throws IOException {
        // 检查图像URL是否有效
        if (imageUrl == null || imageUrl.isEmpty()) {
            throw new IllegalArgumentException("Image URL is null or empty");
        }
        // 修正可能的URL问题
        if (imageUrl.startsWith("//")) {
            imageUrl = "https:" + imageUrl;
        }
        URL url = new URL(imageUrl);
        BufferedImage image = ImageIO.read(url.openStream());
        if (image == null) {
            throw new IOException("Could not load image from URL: " + imageUrl);
        }
        // 初始化计数器
        long blueCount = 0;
        long nonBlueCount = 0;
        // 遍历图像中的所有像素
        for (int x = 0; x < image.getWidth(); x++) {
            for (int y = 0; y < image.getHeight(); y++) {
                int pixel = image.getRGB(x, y);
                int red = (pixel >> 16) & 0xff;
                int green = (pixel >> 8) & 0xff;
                int blue = (pixel) & 0xff;
                // 判断该像素是否为蓝色主导
                if (blue > red && blue > green) {
                    blueCount++;
                } else {
                    nonBlueCount++;
                }
            }
        }
        // 返回图像是否以蓝色为主
        return blueCount > nonBlueCount;
    }

    private static void applyRulesBasedOnProductTitle(JSONObject record,Connection conn){
        // 获取产品标题
        String productTitle = record.optString("title");
        // 提取其他必要信息
        String nick = record.optString("nick");
        String pic_url = record.optString("pic_url");
        String price = record.optString("price"); // 假设价格已经是字符串格式
        String detail_url = record.optString("detail_url");

        // 应用不同的清洗规则
        if (productTitle.contains("蓝芩口服液")) {
            applyRulesForLanqin(record, nick, pic_url, price, detail_url);
        } else if (productTitle.contains("黄芪精")) {
            applyRulesForHuangqijing(record, nick, pic_url, price, detail_url, conn);
        } else if (productTitle.contains("可乐可康")) {
            applyRulesForKelekekang(record, nick, pic_url, price, detail_url, conn);
        } else if (productTitle.contains("星迪")) {
            applyRulesForXingdi(record, nick, pic_url, price, detail_url, conn);
        } else if (productTitle.contains("贝雪")) {
            applyRulesForBeixue(record, nick, pic_url, price, detail_url, conn);
        } else if (productTitle.contains("补肾润肺")) {
            applyRulesForBushenRunfei(record, nick, pic_url, price, detail_url, conn);
        } else if (productTitle.contains("苏黄")) {
            applyRulesForSuhuang(record, nick, pic_url, price, detail_url, conn);
        } else if (productTitle.contains("依帕司他片") || productTitle.contains("唐林")) {
            applyRulesForYipasita(record, nick, pic_url, price, detail_url, conn);
        } else if (productTitle.contains("唐林胶囊")) {
            applyRulesForTanglinJiaonang(record, nick, pic_url, price, detail_url, conn);
        } else if (productTitle.contains("散风通窍滴丸")) {
            applyRulesForSanfengTongqiao(record, nick, pic_url, price, detail_url, conn);
        } else if (productTitle.contains("杏荷止咳")) {
            applyRulesForXingheZhiKe(record, nick, pic_url, price, detail_url, conn);
        } else if (productTitle.contains("卢苏")) {
            applyRulesForLusu(record, nick, pic_url, price, detail_url, conn);
        } else if (productTitle.contains("荜铃胃痛")) {
            applyRulesForBilingWeitong(record, nick, pic_url, price, detail_url, conn);
        }
    }

    // 下面是具体的清洗方法实现，您需要根据实际业务逻辑来填充这些方法
    private static void applyRulesForLanqin(JSONObject record, String storeName, String imageUrl, String totalPrice, String detailUrl) {
        // 定义关键词和价格阈值
        String[] keywords = {"7支", "9支", "12支"};
        double[] priceThresholds = {0, 0, 0}; // 这里使用0作为占位符
        String[] quantityKeywords = {"6支", "12支", "14支"};
        // 根据图片URL判断颜色
        boolean isGreenType = false;
        try {
            if (imageUrl != null && !imageUrl.isEmpty()) {
                if (imageUrl.startsWith("//")) {
                    imageUrl = "https:" + imageUrl; // 处理可能的URL问题
                }
                // 调用颜色判断方法
                isGreenType = isImageBlue(imageUrl);
            } else {
                // 处理imageUrl为空的情况
                System.out.println("警告: 图片URL为空，项目ID：" + record.getLong("id"));
            }
        } catch (IOException e) {
            e.printStackTrace(); // 处理异常情况
        }
        // 如果图片类型为绿色，则不执行后续清洗逻辑
        if (isGreenType) {
            return;
        }
        // 应用基于标题的清洗规则
        applyRulesForLanqin(record, storeName, imageUrl, String.valueOf(totalPrice), detailUrl);
    }


    private static void applyRules(JSONObject record, String storeName, String imageUrl, String totalPrice, String detailUrl, String[] keywords, double[] priceThresholds, String[] quantityKeywords, Connection conn) throws SQLException {
        String productTitle = record.optString("title"); // 安全获取产品标题
        double price = record.optDouble("price", -1); // 安全获取价格，默认为-1
        boolean isNonCompliant = false; // 初始设置为合规
        String screenshotPath = null; // 截图路径初始为空

        // 检查关键字、价格阈值与产品是否一致，如果一致设置为不合规
        for (int i = 0; i < keywords.length; i++) {
            if (productTitle.contains(keywords[i]) && (!storeName.contains("骁柔") || (price > 0 && price < priceThresholds[i]))) {
                isNonCompliant = true; // 标记为不合规
                break; // 一旦发现不合规条件，就退出循环
            }
        }

        // 更新JSONObject
        record.put("compliance", isNonCompliant ? "不合规" : "合规");
        if (screenshotPath != null) {
            record.put("screenshot_path", screenshotPath);
        }

        // 如果需要将结果更新回数据库
        String updateSql = "UPDATE cleaned_data SET compliance = ?, screenshot_path = ? WHERE id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(updateSql)) {
            pstmt.setString(1, isNonCompliant ? "不合规" : "合规");
            pstmt.setString(2, screenshotPath);
            pstmt.setLong(3, record.optLong("id"));
            pstmt.executeUpdate();
        }
    }


    private static void applyRulesForHuangqijing(JSONObject record, String storeName, String imageUrl, String totalPrice, String detailUrl, Connection conn) {
        String[] keywords = {"60支", "30支", "24支", "18支"};
        double[] priceThresholds = {0, 0, 0, 0}; // 使用0作为占位符，因为这些规则不需要价格阈值
        String[] quantityKeywords = {}; // 无需数量关键字
        try {
            applyRules(record, storeName, imageUrl, totalPrice, detailUrl, keywords, priceThresholds, quantityKeywords, conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private static void applyRulesForKelekekang(JSONObject record, String storeName, String imageUrl, String totalPrice, String detailUrl,Connection conn) {
        String[] keywords = {"12支", "18支", "36支"};
        double[] priceThresholds = {0, 0, 0}; // 不需要价格阈值
        String[] quantityKeywords = {}; // 无需数量关键字
        try {
            applyRules(record, storeName, imageUrl, totalPrice, detailUrl, keywords, priceThresholds, quantityKeywords, conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void applyRulesForXingdi(JSONObject record, String storeName, String imageUrl, String totalPrice, String detailUrl,Connection conn) {
        String[] keywords = {"28片", "21片"};
        double[] priceThresholds = {0, 0}; // 不需要价格阈值
        String[] quantityKeywords = {}; // 无需数量关键字
        try {
            applyRules(record, storeName, imageUrl, totalPrice, detailUrl, keywords, priceThresholds, quantityKeywords, conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void applyRulesForBeixue(JSONObject record, String storeName, String imageUrl, String totalPrice, String detailUrl,Connection conn) {
        String[] keywords = {"5片"};
        double[] priceThresholds = {0}; // 不需要价格阈值
        String[] quantityKeywords = {"6片", "18片"};
        try {
            applyRules(record, storeName, imageUrl, totalPrice, detailUrl, keywords, priceThresholds, quantityKeywords, conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void applyRulesForBushenRunfei(JSONObject record, String storeName, String imageUrl, String totalPrice, String detailUrl,Connection conn) {
        String[] keywords = {"5瓶"};
        double[] priceThresholds = {0}; // 不需要价格阈值
        String[] quantityKeywords = {};
        try {
            applyRules(record, storeName, imageUrl, totalPrice, detailUrl, keywords, priceThresholds, quantityKeywords, conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void applyRulesForSuhuang(JSONObject record, String storeName, String imageUrl, String totalPrice, String detailUrl,Connection conn) {
        String[] keywords = {"9粒"};
        double[] priceThresholds = {0}; // 不需要价格阈值
        String[] quantityKeywords = {};
        try {
            applyRules(record, storeName, imageUrl, totalPrice, detailUrl, keywords, priceThresholds, quantityKeywords, conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void applyRulesForYipasita(JSONObject record, String storeName, String imageUrl, String totalPrice, String detailUrl,Connection conn) {
        String[] keywords = {"18片", "10片"};
        double[] priceThresholds = {0, 30.96};
        String[] quantityKeywords = {};
        try {
            applyRules(record, storeName, imageUrl, totalPrice, detailUrl, keywords, priceThresholds, quantityKeywords, conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void applyRulesForTanglinJiaonang(JSONObject record, String storeName, String imageUrl, String totalPrice, String detailUrl,Connection conn) {
        String[] keywords = {"18粒", "10粒"};
        double[] priceThresholds = {0, 33.336};
        String[] quantityKeywords = {};
        try {
            applyRules(record, storeName, imageUrl, totalPrice, detailUrl, keywords, priceThresholds, quantityKeywords, conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void applyRulesForSanfengTongqiao(JSONObject record, String storeName, String imageUrl, String totalPrice, String detailUrl,Connection conn) {
        String[] keywords = {"60丸", "100丸"};
        double[] priceThresholds = {0, 77.4};
        String[] quantityKeywords = {};
        try {
            applyRules(record, storeName, imageUrl, totalPrice, detailUrl, keywords, priceThresholds, quantityKeywords, conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void applyRulesForXingheZhiKe(JSONObject record, String storeName, String imageUrl, String totalPrice, String detailUrl,Connection conn) {
        String[] keywords = {"1瓶"};
        double[] priceThresholds = {0};
        String[] quantityKeywords = {};
        try {
            applyRules(record, storeName, imageUrl, totalPrice, detailUrl, keywords, priceThresholds, quantityKeywords, conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void applyRulesForBilingWeitong(JSONObject record, String storeName, String imageUrl, String totalPrice, String detailUrl,Connection conn) {
        String[] keywords = {"6袋", "9袋"};
        double[] priceThresholds = {0, 0};
        String[] quantityKeywords = {"6袋", "18袋"};
        try {
            applyRules(record, storeName, imageUrl, totalPrice, detailUrl, keywords, priceThresholds, quantityKeywords, conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void applyRulesForLusu(JSONObject record, String storeName, String imageUrl, String totalPrice, String detailUrl,Connection conn) {
        String[] keywords = {"5片", "3片"};
        double[] priceThresholds = {0, 0};
        String[] quantityKeywords = {"3片", "5片"};
        try {
            applyRules(record, storeName, imageUrl, totalPrice, detailUrl, keywords, priceThresholds, quantityKeywords, conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }



    private static void saveCleanedData(Connection conn, JSONObject record) throws SQLException {
        String updateSql = "INSERT INTO tm_cleaned_data (id, num_iid, title, nick, detail_url, pic_url, brand, num, price, properties_name, quantity, sku_id, url_f, compliance, screenshot_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE title=VALUES(title), nick=VALUES(nick), detail_url=VALUES(detail_url), pic_url=VALUES(pic_url), brand=VALUES(brand), num=VALUES(num), price=VALUES(price), properties_name=VALUES(properties_name), quantity=VALUES(quantity), sku_id=VALUES(sku_id), url_f=VALUES(url_f), compliance=VALUES(compliance), screenshot_path=VALUES(screenshot_path);";

        try (PreparedStatement pstmt = conn.prepareStatement(updateSql)) {
            pstmt.setLong(1, record.optLong("id"));
            pstmt.setLong(2, record.optLong("num_iid"));
            pstmt.setString(3, record.optString("title"));
            pstmt.setString(4, record.optString("nick"));
            pstmt.setString(5, record.optString("detail_url"));
            pstmt.setString(6, record.optString("pic_url"));
            pstmt.setString(7, record.optString("brand"));
            pstmt.setInt(8, record.optInt("num"));
            pstmt.setDouble(9, record.optDouble("price"));
            pstmt.setString(10, record.optString("properties_name"));
            pstmt.setInt(11, record.optInt("quantity"));
            pstmt.setLong(12, record.optLong("sku_id"));
            pstmt.setString(13, record.optString("url_f")); // 添加 url_f 参数
            pstmt.setString(14, record.optString("compliance"));
            pstmt.setString(15, record.optString("screenshot_path"));

            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }
}