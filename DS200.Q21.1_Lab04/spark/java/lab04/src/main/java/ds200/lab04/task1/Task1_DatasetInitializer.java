package ds200.lab04.task1;

import ds200.lab04.util.OutputWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class Task1_DatasetInitializer {

    public static void main(String[] args) {
        if (args == null || args.length < 6) {
            System.err.println(
                    "Usage: Task1_DatasetInitializer " +
                    "<Orders.csv> <Customer_List.csv> <Order_Items.csv> <Order_Reviews.csv> <Products.csv> <output.txt>"
            );
            System.exit(1);
        }

        String ordersPath = args[0];
        String customersPath = args[1];
        String orderItemsPath = args[2];
        String reviewsPath = args[3];
        String productsPath = args[4];
        String outputPath = args[5];

        SparkSession spark = SparkSession.builder()
                .appName("DS200-Lab04-Task1")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        try {
            Dataset<Row> orders = readCSV(spark, ordersPath);
            Dataset<Row> customers = readCSV(spark, customersPath);
            Dataset<Row> orderItems = readCSV(spark, orderItemsPath);
            Dataset<Row> reviews = readCSV(spark, reviewsPath);
            Dataset<Row> products = readCSV(spark, productsPath);

            List<String> lines = new ArrayList<>();
            lines.add("=========== TASK 1 ===========");

            appendDatasetInfo(lines, "Orders", orders);
            appendDatasetInfo(lines, "Customer_List", customers);
            appendDatasetInfo(lines, "Order_Items", orderItems);
            appendDatasetInfo(lines, "Order_Reviews", reviews);
            appendDatasetInfo(lines, "Products", products);

            OutputWriter.writeLines(outputPath, lines);

            System.out.println("WRITING TO: " + outputPath);
            System.out.println("WRITE SUCCESS");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            spark.stop();
        }
    }

    private static Dataset<Row> readCSV(SparkSession spark, String path) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("sep", ";")
                .option("encoding", "UTF-8")
                .option("mode", "PERMISSIVE")
                .csv(path);
    }

    private static void appendDatasetInfo(List<String> lines, String datasetName, Dataset<Row> df) {
        lines.add("");
        lines.add("====================================");
        lines.add("DATASET: " + datasetName);
        lines.add("====================================");
        lines.add("Row count: " + df.count());
        lines.add("Column count: " + df.columns().length);
        lines.add("");
        lines.add("SCHEMA:");
        lines.add(df.schema().treeString());
        lines.add("SAMPLE DATA:");

        List<Row> sampleRows = df.limit(5).collectAsList();
        for (Row row : sampleRows) {
            lines.add(row.toString());
        }
    }
}
