package tm;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TmAPIFetcher {
    private static final String API_TEMPLATE = "https://api-gw.onebound.cn/taobao/item_search_tmall/?key=t7888181668&&q=%s&start_price=0&end_price=0&page=%d&cat=0&discount_only=&sort=&page_size=50&seller_info=&nick=&ppath=&imgid=&filter=&&lang=zh-CN&secret=20240130";
    private static final Gson gson = new Gson();
    private static final String DB_URL = "jdbc:mysql://localhost:3307/retail?useSSL=false&serverTimezone=UTC&characterEncoding=UTF-8";
    private static final String USER = "root";
    private static final String PASS = "root1234";

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(10); // Adjust thread pool size as needed
        fetchAndProcessData("蓝芩口服液");
        fetchAndProcessData("黄芪精");
        fetchAndProcessData("葡萄糖酸钙锌口服溶液（可乐可康）");
        fetchAndProcessData("香芍颗粒");
        fetchAndProcessData("星迪");
        fetchAndProcessData("贝雪");
        fetchAndProcessData("补肾润肺口服液");
        fetchAndProcessData("神曲消食口服液");
        fetchAndProcessData("苏黄止咳胶囊");
        fetchAndProcessData("依帕司他片（唐林）");
        fetchAndProcessData("依帕司他胶囊（唐林胶囊）");
        fetchAndProcessData("百安新");
        fetchAndProcessData("荜铃胃痛颗粒");
        fetchAndProcessData("枸橼酸他莫昔芬片");
        fetchAndProcessData("散风通窍滴丸");
        fetchAndProcessData("富马酸卢帕他定片（卢苏）");
        fetchAndProcessData("杏荷止咳糖浆");
    }

    private static void fetchAndProcessData(String keyword) {
        try {
            String encodedQ = URLEncoder.encode(keyword, StandardCharsets.UTF_8.name());
            int pageCount = 1; // 默认至少有一页，将在获取第一页数据后更新
            for (int page = 1; page <= pageCount; page++) {
                String url = String.format(API_TEMPLATE, encodedQ, page);
                Map<String, Object> response = sendRequest(url, keyword);
                if (response == null) {
                    System.out.println("Failed to fetch data for page: " + page + " for keyword: " + keyword);
                    continue; // 获取失败时跳过这一页
                }

                // 检查错误码，确保响应有效
                String errorCode = String.valueOf(response.getOrDefault("error_code", "500"));
                if (!"0000".equals(errorCode)) {
                    System.out.println("Error fetching page: " + page + " for keyword: " + keyword + ", error code: " + errorCode);
                    break; // 出错则终止，或根据错误码做相应处理
                }

                // 第一页时，根据需要更新总页数
                if (page == 1) {
                    Object pagecountObj = response.get("pagecount");
                    if (pagecountObj != null) {
                        try {
                            pageCount = Integer.parseInt(pagecountObj.toString());
                        } catch (NumberFormatException e) {
                            System.err.println("Error parsing pagecount: " + e.getMessage());
                            // 如果转换失败，默认保持pageCount = 1
                        }
                    }
                }

                List<Map<String, Object>> items = (List<Map<String, Object>>) response.get("item");
                if (items == null) {
                    System.out.println("No items found for keyword: " + keyword);
                    continue;
                }

                List<Map<String, String>> processedItems = new ArrayList<>();
                for (Map<String, Object> item : items) {
                    Map<String, String> processedItem = new HashMap<>();
                    processedItem.put("title", String.valueOf(item.get("title")));
                    processedItem.put("price", String.valueOf(item.get("price")));
                    processedItem.put("num_iid", String.valueOf(item.get("num_iid")));
                    processedItem.put("seller", String.valueOf(item.get("seller"))); // 假设seller字段存在
                    processedItem.put("detail_url", String.valueOf(item.get("detail_url")));
                    processedItems.add(processedItem);
                }

                if (!processedItems.isEmpty()) {
                    saveItemsToDatabase(processedItems); // 处理和保存数据
                }
            }
        } catch (Exception e) {
            System.out.println("Exception occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void saveItemsToDatabase(List<Map<String, String>> items) {
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
            String selectSql = "SELECT * FROM item_info_tm WHERE num_iid = ?";
            String insertSql = "INSERT INTO item_info_tm (title, price, num_iid, seller, detail_url, updated_mark, if_update) VALUES (?, ?, ?, ?, ?, TRUE, 0)";
            String updateSqlBase = "UPDATE item_info_tm SET updated_mark = TRUE, if_update = ? "; // 基础更新SQL

            for (Map<String, String> item : items) {
                try (PreparedStatement selectStmt = conn.prepareStatement(selectSql)) {
                    selectStmt.setString(1, item.get("num_iid"));
                    ResultSet rs = selectStmt.executeQuery();
                    if (rs.next()) {
                        // Record exists, check for updates
                        int updatedFlag = 0; // 默认没有更新
                        StringBuilder updateSql = new StringBuilder(updateSqlBase);
                        int index = 1; // 用于动态跟踪参数位置

                        // 检查字段是否更新
                        if ((rs.getString("title") != null ? !rs.getString("title").equals(item.get("title")) : item.get("title") != null)) {
                            updateSql.append(", title = ?");
                            updatedFlag = 1;
                        }
                        // 价格使用BigDecimal进行比较
                        if (item.get("price") != null && (rs.getBigDecimal("price") == null || rs.getBigDecimal("price").compareTo(new BigDecimal(item.get("price"))) != 0)) {
                            updateSql.append(", price = ?");
                            updatedFlag = 1;
                        }
                        if ((rs.getString("seller") != null ? !rs.getString("seller").equals(item.get("seller")) : item.get("seller") != null)) {
                            updateSql.append(", seller = ?");
                            updatedFlag = 1;
                        }
                        if ((rs.getString("detail_url") != null ? !rs.getString("detail_url").equals(item.get("detail_url")) : item.get("detail_url") != null)) {
                            updateSql.append(", detail_url = ?");
                            updatedFlag = 1;
                        }
                        updateSql.append(" WHERE num_iid = ?");

                        // Only update if necessary
                        if (updatedFlag == 1) {
                            try (PreparedStatement updateStmt = conn.prepareStatement(updateSql.toString())) {
                                updateStmt.setInt(index++, updatedFlag); // 设置if_update字段
                                if (updateSql.toString().contains("title = ?")) {
                                    updateStmt.setString(index++, item.get("title"));
                                }
                                if (updateSql.toString().contains("price = ?")) {
                                    updateStmt.setBigDecimal(index++, new BigDecimal(item.get("price")));
                                }
                                if (updateSql.toString().contains("seller = ?")) {
                                    updateStmt.setString(index++, item.get("seller"));
                                }
                                if (updateSql.toString().contains("detail_url = ?")) {
                                    updateStmt.setString(index++, item.get("detail_url"));
                                }
                                updateStmt.setString(index, item.get("num_iid"));
                                updateStmt.executeUpdate();
                            }
                        }
                    } else {
                        // Insert new record
                        try (PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
                            insertStmt.setString(1, item.get("title"));
                            insertStmt.setBigDecimal(2, new BigDecimal(item.get("price")));
                            insertStmt.setString(3, item.get("num_iid"));
                            insertStmt.setString(4, item.get("seller"));
                            insertStmt.setString(5, item.get("detail_url"));
                            insertStmt.executeUpdate();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static final HttpClient httpClient = HttpClient.newHttpClient();

    private static final int MAX_RETRIES = 100; // 最大重试次数，可以根据需要调整
    private static final long RETRY_DELAY_MS = 2000; // 重试之间的延迟，单位为毫秒

    private static Map<String, Object> sendRequest(String urlString, String numIid) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(urlString))
                .header("Accept", "application/json; charset=UTF-8")
                .GET()
                .build();

        for (int retries = 0; retries < MAX_RETRIES; retries++) {
            try {
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                System.out.println("RequestingIID: " + numIid + ", API: " + urlString + ", Status Code: " + response.statusCode() + ", Retry: " + retries);

                if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                    Map<String, Object> responseBody = gson.fromJson(response.body(), new TypeToken<Map<String, Object>>(){}.getType());
                    String errorCode = String.valueOf(responseBody.getOrDefault("error_code", "500"));
                    // Check if error code is "0000" or "2000"
                    if ("0000".equals(errorCode) || "2000".equals(errorCode)) {
                        return responseBody; // Return response if error code is "0000" or "2000"
                    } else {
                        System.out.println("Error Code: " + errorCode + " - Retrying");
                    }
                } else {
                    System.out.println("Response Code: " + response.statusCode() + " - Retrying");
                }

                Thread.sleep(RETRY_DELAY_MS); // 等待一段时间后重试
            } catch (IOException | InterruptedException e) {
                System.out.println("Exception during API call to: " + urlString + ". Message: " + e.getMessage());
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
        }

        System.out.println("Failed to fetch data after " + MAX_RETRIES + " retries for num_iid: " + numIid);
        return null;
    }
}
