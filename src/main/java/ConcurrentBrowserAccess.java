import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;


public class ConcurrentBrowserAccess {


    private static final String DB_URL = "jdbc:mysql://localhost:3307/retail?useSSL=false&serverTimezone=UTC&characterEncoding=UTF-8";
    private static final String USER = "root";
    private static final String PASS = "root1234";
    private static WebDriver driver;

    public static void main(String[] args) {
        System.setProperty("webdriver.chrome.driver", "E:\\chromedriver-win64\\chromedriver.exe");
        driver = new ChromeDriver(); // 浏览器实例在这里初始化

        // 分组的商品列表
        List<List<String>> productGroups = Arrays.asList(
                Arrays.asList("扬子江 枸橼酸他莫昔芬片","扬子江 富马酸卢帕他定片","扬子江 枸地氯雷他定片"),
                Arrays.asList("扬子江 蓝芩口服液"),
                Arrays.asList("扬子江 黄芪精"),
                Arrays.asList("扬子江 苏黄止咳"),
                Arrays.asList("扬子江 氨氯地平贝那普利片","扬子江 杏荷止咳","扬子江 神曲消食口服液"),
                Arrays.asList("扬子江 荜铃胃痛颗粒","扬子江 补肾润肺口服液","扬子江 葡萄糖酸钙锌口服溶液"),
                Arrays.asList("扬子江 散风通窍滴丸","扬子江 依帕司他片","扬子江 香芍颗粒"),
                Arrays.asList("星迪 苯磺酸左氨氯地平片28片","扬子江 依帕司他胶囊")

        );


        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(productGroups.size());

        for (List<String> productGroup : productGroups) {
            // 这里设置每天执行一次任务，你可以根据需要调整首次延迟执行的时间和周期
            executorService.scheduleAtFixedRate(() -> {
                WebDriver driver = new ChromeDriver();
                for (String product : productGroup) {
                    searchAndProcessProduct(driver, product);
                }
            }, 0, 1, TimeUnit.DAYS);
        }
    }

    private static void searchAndProcessProduct(WebDriver driver, String productName) {
        try {
            driver.get("https://search.jd.com/");
            WebElement searchBox = driver.findElement(By.id("keyword"));
            searchBox.clear(); // 清除搜索框中的内容
            searchBox.sendKeys(productName);
            WebElement searchButton = driver.findElement(By.cssSelector("input.input_submit"));
            searchButton.click();

            // 等待用户在命令行输入'c'后继续
            System.out.println("Press 'c' and Enter to continue with " + productName);
            Scanner scanner = new Scanner(System.in);
            while (!scanner.nextLine().trim().equalsIgnoreCase("c")) {
                System.out.println("Invalid input. Please press 'c' and Enter to continue...");
            }

            Thread.sleep(5000); // 等待搜索结果

            int pageCount = 0;
            int maxPageCount = Integer.MAX_VALUE;
            // 对蓝芩口服液扬子江和黄芪精扬子江设置特殊的最大页数限制
            if ("蓝芩口服液".equals(productName) || "黄芪精".equals(productName)) {
                maxPageCount = 20;
            }

            do {
                extractDataAndSave(driver, productName);

                List<WebElement> nextButtons = driver.findElements(By.cssSelector("a.pn-next:not(.disabled)"));
                if (nextButtons.isEmpty() || pageCount >= maxPageCount) {
                    break;
                }

                nextButtons.get(0).click();
                Thread.sleep(5000); // 等待新页面加载
                // 再次等待搜索结果加载
                driver.navigate().refresh();
                Thread.sleep(5000); // 等待页面刷新完成

                pageCount++;
            } while (pageCount < maxPageCount);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void extractDataAndSave(WebDriver driver, String productName) {
        List<WebElement> items = driver.findElements(By.cssSelector("[data-sku]"));
        for (WebElement item : items) {
            String skuId = item.getAttribute("data-sku");
            System.out.println("Processing " + productName + " - Found data-sku value: " + skuId);
            saveSkuToDatabase(skuId);
        }
    }
    private static void saveSkuToDatabase(String skuId) {
        String insertOrUpdateSql = "INSERT INTO sku_table (sku_id) VALUES (?) ON DUPLICATE KEY UPDATE update_time=NOW()";
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
             PreparedStatement pstmt = conn.prepareStatement(insertOrUpdateSql)) {
            pstmt.setString(1, skuId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
        }
    }
}