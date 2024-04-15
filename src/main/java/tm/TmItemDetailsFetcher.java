package tm;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import jd.ItemDetailsFetcher;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class TmItemDetailsFetcher {
    private static final Gson gson = new Gson();
    private static final String API_URL_TEMPLATE = "https://api-gw.onebound.cn/jd/item_get/?key=t7888181668&&num_iid=%s&&lang=zh-CN&secret=20240130";
    private static final String dbUrl = "jdbc:mysql://localhost:3307/retail?useSSL=false&serverTimezone=UTC&characterEncoding=UTF-8";
    private String user = "root";
    private String pass = "root1234";
    private static final int MAX_RETRIES = 100;
    private static final long RETRY_DELAY_MS = 1000;
    private static final int THREAD_POOL_SIZE = 10;
    private static final Semaphore semaphore = new Semaphore(1);

    private List<RequestResult> requestResults = new CopyOnWriteArrayList<>();

    class RequestResult {
        String numIid;
        boolean success;
        String errorMessage;

        public RequestResult(String numIid, boolean success, String errorMessage) {
            this.numIid = numIid;
            this.success = success;
            this.errorMessage = errorMessage;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        TmItemDetailsFetcher fetcher = new TmItemDetailsFetcher();

        List<String> iids = fetcher.getAllIidsFromDatabase();
        for (String iid : iids) {
            executorService.submit(() -> fetcher.fetchAndSaveItemDetailsWithRateLimit(iid));
        }

        executorService.shutdown();
        if (!executorService.awaitTermination(60, TimeUnit.MINUTES)) {
            executorService.shutdownNow();
        }

        fetcher.saveResultsToCsv();
    }

    private void fetchAndSaveItemDetailsWithRateLimit(String numIid) {
        try {
            semaphore.acquire();
            fetchAndSaveItemDetails(numIid);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            semaphore.release();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void fetchAndSaveItemDetails(String numIid) {
        int attempt = 0;
        while (attempt < MAX_RETRIES) {
            try {
                String urlString = String.format(API_URL_TEMPLATE, numIid);
                URL url = new URL(urlString);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");

                System.out.println("Fetching details for num_iid: " + numIid + " (Attempt " + (attempt + 1) + "/" + MAX_RETRIES + ")");

                if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                    try (InputStreamReader reader = new InputStreamReader(conn.getInputStream())) {
                        Map<String, Object> response = gson.fromJson(reader, new TypeToken<Map<String, Object>>(){}.getType());

                        String errorCode = String.valueOf(response.get("error_code"));
                        System.out.println("Error code: " + errorCode); // 打印错误代码

                        if ("5000".equals(errorCode) ) {
                            System.out.println("Error occurred: " + errorCode + ". Retrying...");
                            attempt++;
                            Thread.sleep(RETRY_DELAY_MS);
                            continue;
                        }

                        Map<String, Object> item = (Map<String, Object>) response.get("item");
                        if (item == null || item.isEmpty()) {
                            System.out.println("Item data is null or empty for num_iid: " + numIid);
                            return;
                        }

                        if (item.containsKey("skus") && item.get("skus") instanceof Map) {
                            Map<String, Object> skusMap = (Map<String, Object>) item.get("skus");
                            if (skusMap.containsKey("sku") && skusMap.get("sku") instanceof List) {
                                List<Map<String, Object>> skuList = (List<Map<String, Object>>) skusMap.get("sku");
                                if (!skuList.isEmpty()) {
                                    for (Map<String, Object> sku : skuList) {
                                        saveOrUpdateItemWithSku(item, sku);
                                    }
                                } else {
                                    System.out.println("SKUs data is null or empty for num_iid: " + numIid);
                                }
                            } else {
                                System.out.println("Item does not contain SKUs or SKUs format is not as expected for num_iid: " + numIid);
                            }
                        } else {
                            System.out.println("SKUs information is missing for num_iid: " + numIid);
                        }

                        System.out.println("Successfully processed details for num_iid: " + numIid);
                        return;
                    }
                } else {
                    System.out.println("Failed to fetch data for num_iid: " + numIid + ". HTTP error code: " + conn.getResponseCode());
                    return;
                }
            } catch (Exception e) {
                System.out.println("Exception occurred while fetching details for num_iid: " + numIid);
                e.printStackTrace();
                attempt++;
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        System.out.println("Failed to fetch details after " + MAX_RETRIES + " attempts for num_iid: " + numIid);
    }

    private String safeParseString(Object obj, String defaultValue) {
        if (obj != null) {
            return obj.toString();
        } else {
            return defaultValue;
        }
    }

    private void saveOrUpdateItemWithSku(Map<String, Object> item, Map<String, Object> sku) throws SQLException {
        // 假设 sku_id 是一个大数字，我们要确保它被正确地转换成字符串
        Object skuIdObj = sku.get("sku_id");
        String skuId = skuIdObj instanceof Number ? String.valueOf(((Number)skuIdObj).longValue()) : skuIdObj.toString();

        // 现在使用 skuId 来调用其他方法或进行数据库操作
        if (skuExists(skuId)) {
            updateItemWithSku(item, sku);
        } else {
            // 如果SKU不存在，插入新记录
            insertItemWithSku(item, sku);
        }
    }

    private boolean skuExists(String skuId) throws SQLException {
        String checkSql = "SELECT COUNT(*) FROM tm_item_sku_combined WHERE sku_id = ?";
        try (Connection conn = DriverManager.getConnection(dbUrl, user, pass);
             PreparedStatement stmt = conn.prepareStatement(checkSql)) {
            stmt.setString(1, skuId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        }
        return false;
    }

    private void insertItemWithSku(Map<String, Object> item, Map<String, Object> sku) throws SQLException {
        String sql = "INSERT INTO tm_item_sku_combined (num_iid, title, nick, detail_url, pic_url, brand, num, price, properties_name, quantity, sku_id, sku_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(dbUrl, user, pass);
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            // 设置共通字段
            Object numIidObj = item.get("num_iid");
            String numIidStr = numIidObj instanceof Number ? new BigDecimal(numIidObj.toString()).toPlainString() : numIidObj.toString();
            stmt.setString(1, numIidStr);
            stmt.setString(2, item.get("title").toString());
            stmt.setString(3, item.get("nick").toString());
            stmt.setString(4, (sku != null && sku.get("sku_url") != null) ? sku.get("sku_url").toString() : null);
            stmt.setString(5, item.get("pic_url").toString());
            String brand = item.get("brand") != null ? item.get("brand").toString() : "";
            stmt.setString(6, brand);
            stmt.setInt(7, safeParseInt(item.get("num"), 0));

            // 设置SKU特定字段
            BigDecimal price = (sku != null && sku.get("price") != null) ? new BigDecimal(sku.get("price").toString()) : BigDecimal.ZERO;
            price = price.setScale(2, RoundingMode.HALF_UP);
            stmt.setBigDecimal(8, price);
            stmt.setString(9, (sku != null && sku.get("properties_name") != null) ? sku.get("properties_name").toString() : "");
            stmt.setInt(10, safeParseInt((sku != null && sku.get("quantity") != null) ? sku.get("quantity") : 0, 0));

            // 处理sku_id以避免科学计数法
            // 修改sku_id的处理逻辑
            Object skuIdObj = sku.get("sku_id");
            String skuIdStr;
            if (skuIdObj instanceof Number) {
                skuIdStr = new BigDecimal(skuIdObj.toString()).toPlainString();
            } else {
                skuIdStr = skuIdObj.toString();
            }

            // 使用转换后的skuIdStr设置PreparedStatement的参数
            stmt.setString(11, skuIdStr);

            stmt.setString(12, (sku != null && sku.get("sku_url") != null) ? sku.get("sku_url").toString() : null);

            stmt.executeUpdate();
        }
    }


    private void updateItemWithSku(Map<String, Object> item, Map<String, Object> sku) throws SQLException {
        // 修改后的SQL语句，确保适当地处理所有预期的参数
        String sql = "UPDATE tm_item_sku_combined SET title = ?, nick = ?, detail_url = ?, pic_url = ?, brand = ?, num = ?, price = ?, properties_name = ?, quantity = ?, sku_url = ? WHERE sku_id = ? OR (sku_id IS NULL AND num_iid = ?)";
        try (Connection conn = DriverManager.getConnection(dbUrl, user, pass);
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            // 设置参数
            stmt.setString(1, (String) item.get("title"));
            stmt.setString(2, (String) item.get("nick"));
            stmt.setString(3, (String) item.get("detail_url"));
            stmt.setString(4, (String) item.get("pic_url"));
            String brand = item.get("brand") != null ? item.get("brand").toString() : "";
            stmt.setString(5, brand);
            stmt.setInt(6, safeParseInt(item.get("num"), 0));
            BigDecimal price = new BigDecimal(safeParseString(sku != null ? sku.get("price") : item.get("price"), "0")).setScale(2, RoundingMode.HALF_UP);
            stmt.setBigDecimal(7, price);
            stmt.setString(8, sku != null ? (String) sku.get("properties_name") : null);
            stmt.setInt(9, safeParseInt(sku != null ? sku.get("quantity") : null, 0));

            // 处理sku_id为字符串，确保不使用科学计数法
            Object skuIdObj = sku.get("sku_id");
            String skuIdStr;
            if (skuIdObj instanceof Number) {
                skuIdStr = new BigDecimal(skuIdObj.toString()).toPlainString();
            } else {
                skuIdStr = skuIdObj.toString();
            }

            // 使用转换后的skuIdStr设置PreparedStatement的参数
            stmt.setString(11, skuIdStr);

            stmt.setString(10, sku != null ? safeParseString(sku.get("sku_url"), null) : null);
            stmt.setString(11, skuIdStr);
            // 对 num_iid 的处理，确保不使用科学计数法
            Object numIidObj = item.get("num_iid");
            String numIidStr = numIidObj instanceof Number ? new BigDecimal(numIidObj.toString()).toPlainString() : numIidObj.toString();
            stmt.setString(12, numIidStr);

            stmt.executeUpdate();
        }
    }


    private int safeParseInt(Object obj, int defaultValue) {
        if (obj instanceof Number) {
            return ((Number) obj).intValue();
        } else if (obj != null) {
            try {
                return Integer.parseInt(obj.toString());
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        } else {
            return defaultValue;
        }
    }

    // 新增方法，用于执行SQL查询并获取所有的num_iid
    public List<String> getAllIidsFromDatabase() {
        List<String> iids = new ArrayList<>();
        String complexSql = "select * from sku_table;";
        try (Connection conn = DriverManager.getConnection(dbUrl, user, pass);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(complexSql)) {

            while (rs.next()) {
                iids.add(rs.getString("sku_id"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return iids;
    }

    private void saveResultsToCsv() {
        try (PrintWriter writer = new PrintWriter(new FileWriter("request_results.csv"))) {
            writer.println("numIid,success,errorMessage");
            for (RequestResult result : requestResults) {
                writer.printf("%s,%b,\"%s\"%n", result.numIid, result.success, result.errorMessage.replace("\"", "\"\""));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

