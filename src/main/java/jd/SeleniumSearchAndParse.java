package jd;

import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SeleniumSearchAndParse {

    public static void main(String[] args) {
        // 初始化CSV文件
        FileWriter csvWriter = null;
        File file = new File("output.csv");
        boolean fileExists = file.exists(); // 检查文件是否存在

        try {
            // 如果文件不存在或者文件大小为0，代表是新的或者空的文件，那么我们需要写入标题头
            csvWriter = new FileWriter(file, true); // 启用追加模式
            if (!fileExists) {
                csvWriter.append("Query,Data-SKU\n"); // 只有当文件是新的时候才写入标题头
            }
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        // 读取YAML文件
        Yaml yaml = new Yaml(new Constructor(Map.class));
        Map<String, List<String>> data;
        try {
            data = yaml.load(new FileInputStream("D:\\ApiDataFetcher\\src\\main\\resources\\queries.yml"));
        } catch (IOException e) {
            e.printStackTrace();
            try {
                csvWriter.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            return;
        }
        List<String> queries = data.get("queries");

        for (String query : queries) {
            WebDriver driver = null;
            try {
                // 对于每个查询值，初始化一个新的WebDriver实例
                System.setProperty("webdriver.chrome.driver", "D:\\chromedriver-win64\\chromedriver.exe");
                driver = new ChromeDriver();

                // 访问指定网页
                driver.get("https://www.jd.com/");
                Thread.sleep(30000); // 等待页面加载

                WebElement searchBox = driver.findElement(By.id("key"));
                searchBox.clear();
                searchBox.sendKeys(query);

                WebDriverWait wait = new WebDriverWait(driver, 10); // 等待最多10秒
                WebElement searchButton = wait.until(ExpectedConditions.elementToBeClickable(By.cssSelector("button[aria-label='搜索']")));
                searchButton.click();

                Thread.sleep(20000); // 等待页面加载

                boolean hasNextPage = true;
                while (hasNextPage) {
                    // 解析新页面的结果
                    List<WebElement> items = driver.findElements(By.xpath("//li[contains(@class, 'gl-item')]"));
                    Pattern pattern = Pattern.compile("data-sku=\"(.*?)\"");
                    for (WebElement item : items) {
                        String itemHtml = item.getAttribute("outerHTML");
                        Matcher matcher = pattern.matcher(itemHtml);
                        while (matcher.find()) {
                            String dataSku = matcher.group(1);
                            csvWriter.append(String.join(",", query, dataSku));
                            csvWriter.append("\n");
                        }
                    }

                    // 尝试点击下一页
                    try {
                        WebElement nextPageButton = driver.findElement(By.cssSelector("a.pn-next:not(.disabled)"));
                        if (nextPageButton != null) {
                            ((JavascriptExecutor) driver).executeScript("arguments[0].click();", nextPageButton);
                            Thread.sleep(20000); // 等待页面加载
                        } else {
                            hasNextPage = false;
                        }
                    } catch (Exception e) {
                        hasNextPage = false;
                    }
                }
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            } finally {
                if (driver != null) {
                    driver.quit();
                }
            }
        }

        // 完成所有操作后关闭文件
        try {
            if (csvWriter != null) {
                csvWriter.flush();
                csvWriter.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
