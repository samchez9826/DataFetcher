package jd;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class loadQueries {
    public static List<String> loadQueriesFromYaml(String yamlResourceName) {
        Yaml yaml = new Yaml();
        try (InputStream in = APIFetcher.class.getClassLoader().getResourceAsStream(yamlResourceName)) {
            if (in == null) {
                throw new RuntimeException("Resource not found: " + yamlResourceName);
            }
            Map<String, List<String>> data = yaml.load(in);
            return data.get("queries");
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }
}
