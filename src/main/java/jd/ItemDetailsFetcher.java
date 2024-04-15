package jd;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class ItemDetailsFetcher {
    private static final Gson gson = new Gson();
    private static final String API_URL_TEMPLATE = "https://api-gw.onebound.cn/jd/item_get/?key=t7888181668&&num_iid=%s&&lang=zh-CN&secret=20240130";
    private String dbUrl = "jdbc:mysql://rm-uf6n4777qk62x4p60vo.rwlb.rds.aliyuncs.com:3306/yang_test?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
    private String user = "wzy";
    private String pass = "wuzhiyuan421524YUAO+";
    private static final int MAX_RETRIES = 150; // 最大重试次数
    private static final long RETRY_DELAY_MS = 1000; // 重试之间的延迟，以毫秒为单位
    private static final int THREAD_POOL_SIZE = 10; // 线程池大小，可以根据需要调整
    private static final Semaphore semaphore = new Semaphore(1); // 限制同时只能有一个线程执行API调用

    public void fetchAndSaveItemDetailsWithRateLimit(String numIid) {
        try {
            semaphore.acquire(); // 获取许可
            fetchAndSaveItemDetails(numIid); // 执行原有的API调用和保存逻辑
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            semaphore.release(); // 释放许可
            try {
                Thread.sleep(1000); // 确保1秒钟调用一次API
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
                        if ("5000".equals(errorCode)) {
                            System.out.println("Error occurred: 5000. Retrying...");
                            attempt++;
                            Thread.sleep(RETRY_DELAY_MS);
                            continue;
                        }

                        Map<String, Object> item = (Map<String, Object>) response.get("item");
                        if (item == null || item.isEmpty()) { // 检查item是否为空或没有数据
                            System.out.println("Item data is null or empty for num_iid: " + numIid);
                            return;
                        }

                        // 处理sku逻辑，仅当sku数据存在且有效时
                        Object skusObject = item.get("skus");
                        if (skusObject instanceof Map) {
                            Map<String, Object> skusMap = (Map<String, Object>) skusObject;
                            Object skuListObject = skusMap.get("sku");
                            if (skuListObject instanceof List && !((List<?>) skuListObject).isEmpty()) {
                                List<Map<String, Object>> skus = (List<Map<String, Object>>) skuListObject;
                                for (Map<String, Object> sku : skus) {
                                    saveOrUpdateItemWithSku(item, sku);
                                }
                            } else {
                                System.out.println("SKUs data is null or empty for num_iid: " + numIid);
                            }
                        } else {
                            System.out.println("SKUs format is not as expected for num_iid: " + numIid);
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


    public void saveItemAndSkuToDatabase(Map<String, Object> item, List<Map<String, Object>> skus) throws SQLException {
        if (skus == null || skus.isEmpty()) {
            saveItemWithoutSkuToDatabase(item); // 处理没有SKU的情况
        } else {
            for (Map<String, Object> sku : skus) {
                saveItemWithSkuToDatabase(item, sku); // 处理每个SKU
            }
        }
    }
    private void saveItemWithSkuToDatabase(Map<String, Object> item, Map<String, Object> sku) throws SQLException {
        // 注意：假设数据库表已经调整，添加了sku_url列
        String sqlWithSku = "INSERT INTO item_sku_combined (num_iid, title, nick, detail_url, pic_url, brand, num, price, properties_name, quantity, sku_id, sku_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(dbUrl, user, pass);
             PreparedStatement stmt = conn.prepareStatement(sqlWithSku)) {
            setPreparedStatement(stmt, item, sku); // 调整此方法以接受SKU的sku_url
            stmt.executeUpdate();
        }
    }
    private void saveItemWithoutSkuToDatabase(Map<String, Object> item) throws SQLException {
        String sqlWithoutSku = "INSERT INTO item_sku_combined (num_iid, title, nick, detail_url, pic_url, brand, num, price, properties_name, quantity, sku_id, sku_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)";

        try (Connection conn = DriverManager.getConnection(dbUrl, user, pass);
             PreparedStatement stmt = conn.prepareStatement(sqlWithoutSku)) {
            setPreparedStatement(stmt, item, null); // 传递null作为sku参数
            stmt.executeUpdate();
        }
    }

    private void setPreparedStatement(PreparedStatement stmt, Map<String, Object> item, Map<String, Object> sku) throws SQLException {
        stmt.setString(1, (String) item.get("num_iid"));
        stmt.setString(2, (String) item.get("title"));
        stmt.setString(3, (String) item.get("nick"));
        stmt.setString(4, (String) item.get("detail_url")); // 使用item的detail_url
        stmt.setString(5, (String) item.get("pic_url"));
        stmt.setString(6, (String) item.get("brand"));
        int num = safeParseInt(item.get("num"), 0);
        stmt.setInt(7, num);

        // 根据是否存在SKU来设置价格
        BigDecimal price = sku != null ? new BigDecimal(safeParseString(sku.get("price"), "0")) : new BigDecimal("0");
        price = price.setScale(2, RoundingMode.HALF_UP);
        stmt.setBigDecimal(8, price);

        if (sku != null) {
            stmt.setString(9, (String) sku.get("properties_name"));
            int quantity = safeParseInt(sku.get("quantity"), 0);
            stmt.setInt(10, quantity);
        } else {
            stmt.setNull(9, Types.VARCHAR);
            stmt.setNull(10, Types.INTEGER);
        }

        // SKU ID 只在存在SKU时设置
        if (sku != null) {
            stmt.setString(11, (String) sku.get("sku_id"));
        } else {
            stmt.setNull(11, Types.VARCHAR);
        }
    }

    private String safeParseString(Object obj, String defaultValue) {
        if (obj != null) {
            return obj.toString();
        } else {
            return defaultValue;
        }
    }



    private void saveOrUpdateItemWithSku(Map<String, Object> item, Map<String, Object> sku) throws SQLException {
        String skuId = sku != null ? sku.get("sku_id").toString() : null;
        if (skuId != null && skuExists(skuId)) {
            // 如果SKU存在，更新记录
            updateItemWithSku(item, sku);
        } else {
            // 如果SKU不存在，插入新记录
            insertItemWithSku(item, sku);
        }
    }



    private boolean skuExists(String skuId) throws SQLException {
        String checkSql = "SELECT COUNT(*) FROM item_sku_combined WHERE sku_id = ?";
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
        String sql = "INSERT INTO item_sku_combined (num_iid, title, nick, detail_url, pic_url, brand, num, price, properties_name, quantity, sku_id, sku_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(dbUrl, user, pass);
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            // 设置共通字段
            stmt.setString(1, item.get("num_iid").toString());
            stmt.setString(2, item.get("title").toString());
            stmt.setString(3, item.get("nick").toString());
            // 使用SKU的sku_url而不是item的detail_url，如果sku为null或sku_url不存在，设置为null
            stmt.setString(4, (sku != null && sku.get("sku_url") != null) ? sku.get("sku_url").toString() : null);
            stmt.setString(5, item.get("pic_url").toString());
            stmt.setString(6, item.get("brand").toString());
            stmt.setInt(7, safeParseInt(item.get("num"), 0));

            // 设置SKU特定字段
            BigDecimal price = (sku != null && sku.get("price") != null) ? new BigDecimal(sku.get("price").toString()) : BigDecimal.ZERO;
            price = price.setScale(2, RoundingMode.HALF_UP);
            stmt.setBigDecimal(8, price);
            stmt.setString(9, (sku != null && sku.get("properties_name") != null) ? sku.get("properties_name").toString() : "");
            stmt.setInt(10, safeParseInt((sku != null && sku.get("quantity") != null) ? sku.get("quantity") : 0, 0));
            stmt.setString(11, (sku != null && sku.get("sku_id") != null) ? sku.get("sku_id").toString() : "");
            // 确保为sku_url设置了值，即使它是null
            stmt.setString(12, (sku != null && sku.get("sku_url") != null) ? sku.get("sku_url").toString() : null);

            stmt.executeUpdate();
        }
    }

    private void updateItemWithSku(Map<String, Object> item, Map<String, Object> sku) throws SQLException {
        // 修改后的SQL语句，确保适当地处理所有预期的参数
        String sql = "UPDATE item_sku_combined SET title = ?, nick = ?, detail_url = ?, pic_url = ?, brand = ?, num = ?, price = ?, properties_name = ?, quantity = ?, sku_url = ? WHERE sku_id = ? OR (sku_id IS NULL AND num_iid = ?)";
        try (Connection conn = DriverManager.getConnection(dbUrl, user, pass);
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            // 设置参数
            stmt.setString(1, (String) item.get("title"));
            stmt.setString(2, (String) item.get("nick"));
            stmt.setString(3, (String) item.get("detail_url"));
            stmt.setString(4, (String) item.get("pic_url"));
            stmt.setString(5, (String) item.get("brand"));
            stmt.setInt(6, safeParseInt(item.get("num"), 0));
            BigDecimal price = new BigDecimal(safeParseString(sku != null ? sku.get("price") : item.get("price"), "0")).setScale(2, RoundingMode.HALF_UP);
            stmt.setBigDecimal(7, price);
            stmt.setString(8, sku != null ? (String) sku.get("properties_name") : null);
            stmt.setInt(9, safeParseInt(sku != null ? sku.get("quantity") : null, 0));
            // 这里是关键的改动，即使sku为null，我们也为sku_url和sku_id设置了适当的值
            stmt.setString(10, sku != null ? safeParseString(sku.get("sku_url"), null) : null);
            stmt.setString(11, sku != null ? safeParseString(sku.get("sku_id"), null) : null);
            stmt.setString(12, (String) item.get("num_iid")); // 使用num_iid作为替代条件

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



    public static void main(String[] args) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE); // 创建固定大小的线程池
        ItemDetailsFetcher fetcher = new ItemDetailsFetcher();

        // 直接指定CSV文件路径
        String csvFilePath = "C:\\Users\\samchez\\Desktop\\1.csv"; // 请确保路径和文件名正确

        List<String> iids = fetcher.getAllIidsFromCsv(csvFilePath);

        for (String iid : iids) {
            final String numIid = iid;
            executorService.submit(() -> fetcher.fetchAndSaveItemDetailsWithRateLimit(numIid)); // 提交任务到线程池
        }

        executorService.shutdown(); // 关闭线程池
        try {
            if (!executorService.awaitTermination(60, TimeUnit.MINUTES)) {
                executorService.shutdownNow(); // 等待任务完成，或者超时强制关闭
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }

    // 从CSV文件中读取num_iid的方法，其他方法保持原样
    public List<String> getAllIidsFromCsv(String csvFilePath) {
        List<String> iids = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                iids.add(line.trim());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return iids;
    }
}
