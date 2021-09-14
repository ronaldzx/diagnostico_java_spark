package minsait.ttaa.datio.utils;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class PropertiesUtils {
    public static void main(String[] args) throws IOException {
        FileReader reader = new FileReader("application.properties");
        Properties prop = new Properties();
        prop.load(reader);

        String mode = prop.getProperty("spark.mode");
        String header = prop.getProperty("spark.header");
        String inferSchema = prop.getProperty("spark.infer.schema");
        String inputPath = prop.getProperty("spark.input.path");
        String outputPath = prop.getProperty("spark.output.path");
    }
}
