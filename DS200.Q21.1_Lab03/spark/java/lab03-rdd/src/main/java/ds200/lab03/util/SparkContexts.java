package ds200.lab03.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.File;

public final class SparkContexts {
    private SparkContexts() {}

    public static JavaSparkContext localContext(String appName) {
        // Use a local directory (absolute) for warehouse; do NOT prepend file:///
        String currentDir = new File(".").getAbsoluteFile().getParentFile().getAbsolutePath();

        var conf = new SparkConf()
            .setAppName(appName)
            .setMaster("local[*]")
            .set("spark.ui.enabled", "false")
            .set("spark.sql.warehouse.dir", currentDir);

        return new JavaSparkContext(conf);
    }
}
