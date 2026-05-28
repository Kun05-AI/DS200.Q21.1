package ds200.lab04.task2;

import ds200.lab04.util.OutputWriter;
import ds200.lab04.util.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public final class Task2_GeneralStatistics {
    private Task2_GeneralStatistics() {}

    public static void main(String[] args) {
        if (args == null || args.length < 6) {
            System.err.println("Usage: Task2_GeneralStatistics <orders.csv> <customer_list.csv> <order_items.csv> <order_reviews.csv> <products.csv> <out>");
            System.exit(1);
        }

        String ordersPath = args[0];
        String customersPath = args[1];
        String itemsPath = args[2];
        String reviewsPath = args[3];
        String productsPath = args[4];
        String outPath = args[5];

        SparkSession spark = SparkSessionFactory.create("Task2_GeneralStatistics");

        try {
            Dataset<Row> orders = readCsv(spark, ordersPath);
            Dataset<Row> customers = readCsv(spark, customersPath);
            Dataset<Row> items = readCsv(spark, itemsPath);
            Dataset<Row> reviews = readCsv(spark, reviewsPath);
            Dataset<Row> products = readCsv(spark, productsPath);

            long totalOrders = orders.select("Order_ID").distinct().count();
            long totalCustomers = customers.select("Subscriber_ID").distinct().count();
            long totalSellers = items.select("Seller_ID").distinct().count();

            List<String> lines = new ArrayList<>();
            lines.add("Metric|Value");
            lines.add("Total_Orders|" + totalOrders);
            lines.add("Total_Customers|" + totalCustomers);
            lines.add("Total_Sellers|" + totalSellers);

            OutputWriter.writeLines(outPath, lines);
            System.out.println("Task 2 wrote " + lines.size() + " lines to " + outPath);
        } finally {
            spark.stop();
        }
    }

    private static Dataset<Row> readCsv(SparkSession spark, String path) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("sep", ";")
                .option("encoding", "UTF-8")
                .option("mode", "PERMISSIVE")
                .csv(path);
    }
}