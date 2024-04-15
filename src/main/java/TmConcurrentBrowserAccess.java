import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Arrays;
import java.util.List;


public class TmConcurrentBrowserAccess {


    private static final String DB_URL = "jdbc:mysql://localhost:3307/retail?useSSL=false&serverTimezone=UTC&characterEncoding=UTF-8";
    private static final String USER = "root";
    private static final String PASS = "root1234";
    private static WebDriver driver;

    public static void main(String[] args) {
        System.setProperty("webdriver.chrome.driver", "D:\\chromedriver-win64\\chromedriver.exe");

        final ChromeOptions options = new ChromeOptions();
        options.addArguments("--disable-blink-features=AutomationControlled");
        options.addArguments("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36");
        options.addArguments("--disable-extensions");
        options.addArguments("--profile-directory=Default");
        options.addArguments("--incognito");
        options.addArguments("--disable-plugins-discovery");
        options.addArguments("--start-maximized");
        options.addArguments("--auto-open-devtools-for-tabs");
        options.addArguments("--blink-settings=imagesEnabled=false"); // Disabling image loading to speed up the browsing.
        options.setExperimentalOption("excludeSwitches", Arrays.asList("enable-automation"));
        options.setExperimentalOption("useAutomationExtension", false);

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
            executorService.scheduleAtFixedRate(() -> {
                WebDriver driver = new ChromeDriver(options); // Using the same ChromeOptions configuration
                try {
                    for (String product : productGroup) {
                        searchAndProcessProduct(driver, product);
                    }
                } finally {
                    driver.quit(); // Ensure WebDriver instance is closed on completion
                }
            }, 0, 1, TimeUnit.DAYS);
        }
    }

    private static void searchAndProcessProduct(WebDriver driver, String productName) {
        try {
            driver.get("https://search.taobao.com/");
            WebElement searchBox = driver.findElement(By.id("q"));
            searchBox.clear(); // 清除搜索框中的内容
            searchBox.sendKeys(productName); // productName是你希望搜索的商品名称变量

            // 搜索按钮现在是一个<button>元素，其类名为'btn-search'
            WebElement searchButton = driver.findElement(By.className("btn-search"));
            searchButton.click(); // 点击搜索按钮

            // 等待用户在命令行输入'c'后继续
            System.out.println("Press 'c' and Enter to continue with " + productName);
            Scanner scanner = new Scanner(System.in);
            while (!scanner.nextLine().trim().equalsIgnoreCase("c")) {
                System.out.println("Invalid input. Please press 'c' and Enter to continue...");
            }

            Thread.sleep(5000); // 等待搜索结果

            int pageCount = 0;
            int maxPageCount = Integer.MAX_VALUE;
            do {
                extractDataAndSave(driver, productName);
                // 查找下一页按钮
                List<WebElement> nextButtons = driver.findElements(By.cssSelector("button.next-pagination-item.next-next"));
                // 检查是否存在下一页按钮且按钮没有被禁用（即没有disabled属性）
                boolean hasNextPage = !nextButtons.isEmpty() && nextButtons.get(0).isEnabled();
                if (!hasNextPage || pageCount >= maxPageCount) {
                    break;
                }
                nextButtons.get(0).click(); // 点击下一页按钮
                Thread.sleep(5000); // 等待新页面加载
                // 为确保页面已经完全加载，再次刷新页面
                driver.navigate().refresh();
                Thread.sleep(5000); // 等待页面刷新完成
                pageCount++;
            } while (pageCount < maxPageCount);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void extractDataAndSave(WebDriver driver, String productName) {
        // 使用类名选择器来定位所有<a>标签
        List<WebElement> items = driver.findElements(By.cssSelector("a.Card--doubleCardWrapper--L2XFE73"));
        for (WebElement item : items) {
            String itemUrl = item.getAttribute("href");

            // 使用正则表达式从itemUrl中提取id值
            Pattern pattern = Pattern.compile("id=(\\d+)");
            Matcher matcher = pattern.matcher(itemUrl);

            if (matcher.find()) {
                String skuId = matcher.group(1);
                System.out.println("Processing " + productName + " - Found SKU ID: " + skuId);
                saveSkuToDatabase(skuId);
            } else {
                System.out.println("SKU ID not found for " + productName);
            }
        }
    }

    private static void saveSkuToDatabase(String skuId) {
        String insertOrUpdateSql = "INSERT INTO tm_sku_table (sku_id) VALUES (?) ON DUPLICATE KEY UPDATE update_time=NOW()";
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
             PreparedStatement pstmt = conn.prepareStatement(insertOrUpdateSql)) {
            pstmt.setString(1, skuId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
        }
    }
}