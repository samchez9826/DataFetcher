package jd;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.net.HttpURLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class APIFetcher {
    private static final String API_TEMPLATE = "https://api-gw.onebound.cn/jd/item_search/?key=t7888181668&&q=%s&start_price=0&end_price=0&page=%d&cat=0&discount_only=&sort=&seller_info=no&nick=&seller_info=&nick=&ppath=&imgid=&filter=&&lang=zh-CN&secret=20240130";
    private static final Gson gson = new Gson();
    private static final String DB_URL = "jdbc:mysql://rm-uf6n4777qk62x4p60vo.rwlb.rds.aliyuncs.com:3306/yang_test?useSSL=false&serverTimezone=UTC&characterEncoding=UTF-8";
    private static final String USER = "wzy";
    private static final String PASS = "wuzhiyuan421524YUAO+";

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(10); // Adjust thread pool size as needed
//        fetchAndProcessData("扬子江 蓝芩口服液");只有第一页
//        fetchAndProcessData("扬子江 黄芪精");只有第一页
//        fetchAndProcessData("扬子江 可乐可康");网页有一两个 返回2000
//        fetchAndProcessData("扬子江 香芍颗粒");数据返回驴头不对马嘴；删掉扬子江  返回2000
//        fetchAndProcessData("扬子江 星迪");数据返回驴头不对马嘴；删掉扬子江  返回2000
//        fetchAndProcessData("扬子江 贝雪");数据返回驴头不对马嘴；删掉扬子江  返回2000
//        fetchAndProcessData("扬子江 补肾润肺");网页有很多品  返回2000
//        fetchAndProcessData("扬子江 神曲消食口服液");网页有一两个 返回2000
//        fetchAndProcessData("苏黄止咳胶囊"); 数据返回驴头不对马嘴；删掉扬子江  返回2000
//        fetchAndProcessData("依帕司他片（唐林）");网页有一两个 返回2000；
//        fetchAndProcessData("依帕司他胶囊（唐林胶囊）");网页有一两个 返回2000；
//        fetchAndProcessData("百安新");网页有一两个 返回2000；
//        fetchAndProcessData("扬子江 荜铃胃痛颗粒");数据返回驴头不对马嘴；删掉扬子江  返回2000
//        fetchAndProcessData("枸橼酸他莫昔芬片");数据返回驴头不对马嘴；删掉扬子江  返回2000
//        fetchAndProcessData("散风通窍滴丸");;数据返回驴头不对马嘴；删掉扬子江  返回2000
//        fetchAndProcessData("卢苏");;数据返回驴头不对马嘴；删掉扬子江  返回2000
//        fetchAndProcessData("杏荷止咳糖浆");;数据返回驴头不对马嘴；删掉扬子江  返回2000
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

                // 解析并处理获取到的数据
                Map<String, Object> itemsContainer = (Map<String, Object>) response.get("items");
                if (itemsContainer == null || !itemsContainer.containsKey("pagecount")) {
                    System.out.println("The 'items' object is null or missing 'pagecount' for keyword: " + keyword);
                    break;
                }

                // 只在第一页时设置总页数
                if (page == 1) {
                    pageCount = ((Number) itemsContainer.get("pagecount")).intValue();
                }

                List<Map<String, Object>> apiItems = (List<Map<String, Object>>) itemsContainer.get("items");
                List<Map<String, String>> items = new ArrayList<>();
                if (apiItems != null) {
                    for (Map<String, Object> apiItem : apiItems) {
                        Map<String, String> item = new HashMap<>();
                        item.put("title", apiItem.get("title") != null ? apiItem.get("title").toString() : null);
                        // 特别注意价格的处理
                        Object priceObj = apiItem.get("price");
                        String priceStr = priceObj != null ? priceObj.toString() : null;
                        item.put("price", priceStr);
                        item.put("num_iid", apiItem.get("num_iid") != null ? apiItem.get("num_iid").toString() : null);
                        item.put("seller", apiItem.get("seller") != null ? apiItem.get("seller").toString() : null);
                        item.put("detail_url", apiItem.get("detail_url") != null ? apiItem.get("detail_url").toString() : null);
                        items.add(item);
                    }
                }

                if (!items.isEmpty()) {
                    saveItemsToDatabase(items); // 处理和保存数据
                }
            }
        } catch (Exception e) {
            System.out.println("Exception occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }



    private static void saveItemsToDatabase(List<Map<String, String>> items) {
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
            String selectSql = "SELECT * FROM item_info WHERE num_iid = ?";
            String insertSql = "INSERT INTO item_info (title, price, num_iid, seller, detail_url, updated_mark, if_update) VALUES (?, ?, ?, ?, ?, TRUE, 0)";
            String updateSqlBase = "UPDATE item_info SET updated_mark = TRUE, if_update = ? "; // 基础更新SQL

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
                    if ("0000".equals(errorCode)) {
                        return responseBody;
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
