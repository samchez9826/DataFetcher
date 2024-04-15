package jd;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.openqa.selenium.*;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.chrome.ChromeDriver;
import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.*;

import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

public class ScreenshotUtility {
    private static boolean isFirstScreenshotTaken = false;
    static final String CSV_PATH = "E:\\opt\\output\\transformed_data2.csv";
    private static ExecutorService executorService = Executors.newFixedThreadPool(5);
    private static List<WebDriver> webDriverPool = new ArrayList<>();
    private static final String SCREENSHOT_DIR = "E:\\opt\\output\\screenshots";
    private static final String JD_LOGIN_TITLE = "京东-欢迎登录";
    private static final String JD_HOME_TITLE = "京东(JD.COM)-正品低价、品质保障、配送及时、轻松购物！";

    public static void main(String[] args) {
        System.setProperty("webdriver.chrome.driver", "E:\\chromedriver-win64\\chromedriver.exe");
        for (int i = 0; i < 5; i++) {
            webDriverPool.add(new ChromeDriver());
        }

        String filePath = "E:\\opt\\output\\today_pc_links.txt";
        watchAndProcessUrls(filePath);
    }

    private static void watchAndProcessUrls(String filePath) {
        Path path = Paths.get(filePath).getParent();
        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            path.register(watchService, ENTRY_MODIFY);
            while (true) {
                WatchKey key = watchService.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    if (kind == OVERFLOW) {
                        continue;
                    }

                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path filename = ev.context();

                    if (filename.toString().equals(new File(filePath).getName())) {
                        System.out.println("File " + filePath + " has changed!");
                        processUrls(filePath);
                    }
                }
                boolean valid = key.reset();
                if (!valid) {
                    break;
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }


    private static void processUrls(String filePath) {
        Set<String> urls = readUrlsFromFile(filePath);
        AtomicInteger counter = new AtomicInteger(0);

        urls.forEach(url -> {
            executorService.submit(() -> {
                try {
                    String screenshotPath;
                    if (!urlToScreenshotPath.containsKey(url)) {
                        WebDriver driver = webDriverPool.get(counter.getAndIncrement() % webDriverPool.size());
                        String[] result = takeScreenshot(driver, url, SCREENSHOT_DIR);
                        screenshotPath = result[0];
                        urlToScreenshotPath.put(url, screenshotPath);
                    } else {
                        screenshotPath = urlToScreenshotPath.get(url);
                    }
                    updateImageTimestamp(screenshotPath);
                    System.out.println("Screenshot for URL " + url + " updated with timestamp.");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        });
    }


    private static Map<String, String> urlToScreenshotPath = new ConcurrentHashMap<>();

    private static boolean isUrlProcessed(String url) {
        return urlToScreenshotPath.containsKey(url);
    }

    private static String[] takeScreenshot(WebDriver driver, String url, String dirPath) {
        try {
            driver.get(url);
            Thread.sleep(isFirstScreenshotTaken ? 20000 : 30000);
            isFirstScreenshotTaken = true;

            String title = driver.getTitle();
            if (title.contains(JD_LOGIN_TITLE)) {
                // Refresh every 3 minutes until price found or title changes
                while (title.contains(JD_LOGIN_TITLE)) {
                    System.out.println("Title is " + JD_LOGIN_TITLE + ", refreshing every 3 minutes...");
                    Thread.sleep(180000); // 3 minutes
                    driver.navigate().refresh();
                    title = driver.getTitle();
                }
            } else if (title.contains(JD_HOME_TITLE)) {
                // If title is JD home page, reprocess URL list
                System.out.println("Title is " + JD_HOME_TITLE + ", reprocessing URL list...");
                return new String[]{"", ""}; // Return empty array to indicate no screenshot taken
            }

            // Check if URL already processed
            if (isUrlProcessed(url)) {
                System.out.println("URL already processed: " + url);
                return new String[]{"", ""}; // Return empty array to indicate no screenshot taken
            }

            // Try finding the price
            String productId = url.substring(url.lastIndexOf("/") + 1, url.indexOf(".html"));
            String regex = "<span class=\"price J-p-" + productId + "\">([0-9]+\\.[0-9]{2})?</span>";
            Pattern pattern = Pattern.compile(regex);

            boolean priceFound = false;
            int attempts = 0;
            while (!priceFound && attempts < 10) {
                String pageSource = driver.getPageSource();
                Matcher matcher = pattern.matcher(pageSource);
                if (matcher.find() && matcher.group(1) != null) {
                    System.out.println("Price found: " + matcher.group(1));
                    priceFound = true;
                } else {
                    System.out.println("Price not found, refreshing the page...");
                    driver.navigate().refresh();
                    Thread.sleep(5000); // Wait for page to reload
                    attempts++;
                }
            }
            if (!priceFound) {
                System.out.println("Failed to find the price after several attempts.");
                return new String[]{"", ""}; // Return empty array to indicate no screenshot taken
            }

            // Capture screenshot
            File screenshotFile = ((TakesScreenshot) driver).getScreenshotAs(OutputType.FILE);
            BufferedImage image = ImageIO.read(screenshotFile);

            // Generate screenshot path
            String screenshotPath = Paths.get(dirPath, "screenshot_" + System.currentTimeMillis() + ".png").toString();

            // Save screenshot
            ImageIO.write(image, "png", new File(screenshotPath));
            System.out.println("Screenshot saved at: " + screenshotPath);

            // Update URL and screenshot path mapping
            urlToScreenshotPath.put(url, screenshotPath);

            // Check stock status (assuming it's implemented)
            String stockStatus = checkStockStatus(driver);

            // Return screenshot path and stock status
            return new String[]{screenshotPath, stockStatus};
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
        return new String[]{"", ""}; // Return empty array in case of exception
    }

    private static String checkStockStatus(WebDriver driver) {
        try {
            WebElement stockElement = driver.findElement(By.id("store-prompt"));
            if (stockElement.getText().contains("有货") || stockElement.getText().contains("该地区不支持销售")) {
                return "有货";
            }
        } catch (NoSuchElementException e) {
            System.out.println("库存状态元素未找到");
        }
        return "无货";
    }

    private static void updateImageTimestamp(String imagePath) {
        try {
            BufferedImage image = ImageIO.read(new File(imagePath));
            Graphics2D g2d = image.createGraphics();
            g2d.setFont(new Font("Arial", Font.BOLD, 30));
            g2d.setColor(Color.BLUE);
            String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
            g2d.drawString(timeStamp, 10, image.getHeight() - 10);
            g2d.dispose();
            ImageIO.write(image, "png", new File(imagePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Set<String> readUrlsFromFile(String filePath) {
        Set<String> urls = new HashSet<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                urls.add(line.trim());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return urls;
    }

    public static void updateScreenshotPathInCSV(String url, String screenshotPath) throws IOException {
        // 读取CSV文件
        List<CSVRecord> records;
        try (Reader in = new FileReader(CSV_PATH)) {
            records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in).getRecords();
        }

        // 创建一个新的CSV记录列表以保存更新后的记录
        List<String[]> updatedRecords = new ArrayList<>();
        updatedRecords.add(records.get(0).getParser().getHeaderNames().toArray(new String[0])); // 添加标题行

        int urlColumnIndex = -1;
        int screenshotColumnIndex = -1;
        String[] headers = updatedRecords.get(0);
        for (int i = 0; i < headers.length; i++) {
            if ("pc链接（可点击）".equals(headers[i].trim())) {
                urlColumnIndex = i;
            } else if ("截图路径".equals(headers[i].trim())) {
                screenshotColumnIndex = i;
            }
        }

        for (CSVRecord record : records) {
            String[] row = new String[record.size()];
            for (int i = 0; i < record.size(); i++) {
                row[i] = record.get(i);
            }

            if (row[urlColumnIndex].trim().equals(url)) {
                row[screenshotColumnIndex] = screenshotPath; // 更新截图路径
            }

            updatedRecords.add(row);
        }

        // 写回CSV文件
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(CSV_PATH));
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(headers))) {
            for (String[] record : updatedRecords) {
                csvPrinter.printRecord((Object[]) record);
            }
        }
    }
}

