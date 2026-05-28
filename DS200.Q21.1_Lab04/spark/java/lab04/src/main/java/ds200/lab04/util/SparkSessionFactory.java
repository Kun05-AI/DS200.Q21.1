package ds200.lab04.util;

import org.apache.spark.sql.SparkSession;

public final class SparkSessionFactory {
    private SparkSessionFactory() {}

    public static SparkSession create(String appName) {
        return SparkSession.builder()
                .appName(appName)
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();
    }
}
