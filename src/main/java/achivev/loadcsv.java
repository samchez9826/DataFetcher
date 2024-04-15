package achivev;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class loadcsv {
    private static final String API_TEMPLATE = "https://api-gw.onebound.cn/taobao/item_search_tmall/?key=t7888181668&&q=%s&start_price=0&end_price=0&page=%d&cat=0&discount_only=&sort=&page_size=50&seller_info=&nick=&ppath=&imgid=&filter=&&lang=zh-CN&secret=20240130";
    private static final Gson gson = new Gson();
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final int MAX_RETRIES = 3; // 减少重试次数以避免长时间阻塞
    private static final long RETRY_DELAY_MS = 2000;

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(10);
        fetchAndProcessData("枸地氯雷他定片（贝雪）");
        fetchAndProcessData("贝雪");
    }

    private static void fetchAndProcessData(String keyword) {
        try {
            String encodedQ = URLEncoder.encode(keyword, StandardCharsets.UTF_8.name());
            int page = 1;
            int pageCount = Integer.MAX_VALUE; // 假设一个很大的页数，直到从第一页获取实际的页数

            while (page <= pageCount) {
                String url = String.format(API_TEMPLATE, encodedQ, page);
                HttpResponse<String> response = sendRequest(url);
                if (response == null || response.statusCode() != HttpURLConnection.HTTP_OK) {
                    System.out.println("获取数据失败，关键词：" + keyword + "，页码：" + page);
                    break; // 遇到错误时中断循环
                }

                String jsonResponse = response.body();
                Map<String, Object> respMap = gson.fromJson(jsonResponse, new TypeToken<Map<String, Object>>() {}.getType());
                if (page == 1) {
                    Object pageCountObj = respMap.get("pagecount");
                    if (pageCountObj != null) {
                        try {
                            pageCount = Integer.parseInt(pageCountObj.toString());
                        } catch (NumberFormatException e) {
                            System.err.println("无法解析页数：" + e.getMessage());
                        }
                    }
                }


                saveJsonToFile(jsonResponse + "\n", "items_all.json");
                page++;
            }
        } catch (Exception e) {
            System.out.println("发生异常：" + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void saveJsonToFile(String jsonContent, String fileName) {
        try (FileWriter writer = new FileWriter(fileName, true)) {
            writer.write(jsonContent);
        } catch (IOException e) {
            System.err.println("写入文件出错：" + e.getMessage());
        }
    }

    private static HttpResponse<String> sendRequest(String urlString) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(urlString))
                .header("Accept", "application/json; charset=UTF-8")
                .GET()
                .build();

        for (int retries = 0; retries < MAX_RETRIES; retries++) {
            try {
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                    return response;
                } else {
                    Thread.sleep(RETRY_DELAY_MS);
                }
            } catch (IOException | InterruptedException e) {
                System.out.println("请求API出错：" + urlString + "，信息：" + e.getMessage());
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
        }

        System.out.println("多次重试失败，URL：" + urlString);
        return null;
    }
}