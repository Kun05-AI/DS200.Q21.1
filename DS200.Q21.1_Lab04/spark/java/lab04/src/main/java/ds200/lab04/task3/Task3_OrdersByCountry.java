package ds200.lab04.task3;

import ds200.lab04.util.OutputWriter;
import ds200.lab04.util.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public final class Task3_OrdersByCountry {
    private Task3_OrdersByCountry() {}

    public static void main(String[] args) {
        if (args == null || args.length < 3) {
            System.err.println("Usage: Task3_OrdersByCountry <orders.csv> <customer_list.csv> <out>");
            System.exit(1);
        }

        String ordersPath = args[0];
        String customersPath = args[1];
        String outPath = args[2];

        SparkSession spark = SparkSessionFactory.create("Task3_OrdersByCountry");

        try {
            Dataset<Row> orders = readCsv(spark, ordersPath);
            Dataset<Row> customers = readCsv(spark, customersPath);

            Dataset<Row> result = orders
                    .join(customers, "Customer_Trx_ID")
                    .withColumn("Customer_Country", coalesce(col("Customer_Country"), lit("Unknown")))
                    .groupBy("Customer_Country")
                    .agg(countDistinct("Order_ID").alias("Order_Count"))
                    .orderBy(desc("Order_Count"), asc("Customer_Country"));

            List<String> lines = new ArrayList<>();
            lines.add("Customer_Country|Order_Count");

            for (Row r : result.collectAsList()) {
                lines.add(r.getAs("Customer_Country") + "|" + r.getAs("Order_Count"));
            }

            OutputWriter.writeLines(outPath, lines);
            System.out.println("Task 3 wrote " + lines.size() + " lines to " + outPath);
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