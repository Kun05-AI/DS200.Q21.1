package ds200.lab04.task8;

import ds200.lab04.util.OutputWriter;
import ds200.lab04.util.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public final class Task8_DeliveryPerformance {
    private Task8_DeliveryPerformance() {}

    public static void main(String[] args) {
        if (args == null || args.length < 3) {
            System.err.println("Usage: Task8_DeliveryPerformance <orders.csv> <order_items.csv> <out>");
            System.exit(1);
        }

        String ordersPath = args[0];
        String itemsPath = args[1];
        String outPath = args[2];

        SparkSession spark = SparkSessionFactory.create("Task8_DeliveryPerformance");

        try {
            Dataset<Row> orders = readCsv(spark, ordersPath)
                    .withColumn(
                            "Order_Delivered_Carrier_Date",
                            to_timestamp(col("Order_Delivered_Carrier_Date"), "yyyy-MM-dd HH:mm")
                    );

            Dataset<Row> items = readCsv(spark, itemsPath)
                    .withColumn(
                            "Shipping_Limit_Date",
                            to_timestamp(col("Shipping_Limit_Date"), "yyyy-MM-dd HH:mm")
                    );

            Dataset<Row> joined = items
                    .select(
                            col("Order_ID"),
                            col("Product_ID"),
                            col("Seller_ID"),
                            col("Shipping_Limit_Date")
                    )
                    .join(
                            orders.select(
                                    col("Order_ID"),
                                    col("Order_Delivered_Carrier_Date")
                            ),
                            "Order_ID",
                            "left"
                    )
                    // 1. TỐI ƯU: Lọc null trước để thu hẹp tập dữ liệu ngay sau khi Join
                    .filter(
                            col("Shipping_Limit_Date").isNotNull()
                            .and(col("Order_Delivered_Carrier_Date").isNotNull())
                    )
                    // 2. Tính toán cột mới trên tập dữ liệu đã sạch null
                    .withColumn(
                            "Delivery_Delay_Days",
                            datediff(
                                    to_date(col("Order_Delivered_Carrier_Date")),
                                    to_date(col("Shipping_Limit_Date"))
                            )
                    )
                    // 3. Lọc bỏ dữ liệu bất thường (Outliers) từ -365 đến 365 ngày
                    .filter(
                            col("Delivery_Delay_Days").between(-365, 365)
                    )
                    // 4. Khử trùng lặp dữ liệu (Duplicates)
                    .dropDuplicates(
                            "Order_ID",
                            "Product_ID",
                            "Seller_ID",
                            "Shipping_Limit_Date",
                            "Order_Delivered_Carrier_Date"
                    );

            long totalItems = joined.count();
            long lateCount = joined.filter(col("Delivery_Delay_Days").gt(0)).count();
            long onTimeCount = joined.filter(col("Delivery_Delay_Days").equalTo(0)).count();
            long earlyCount = joined.filter(col("Delivery_Delay_Days").lt(0)).count();

            Row statRow = joined.agg(
                    avg("Delivery_Delay_Days").alias("avg_delay"),
                    min("Delivery_Delay_Days").alias("min_delay"),
                    max("Delivery_Delay_Days").alias("max_delay")
            ).first();

            Double avgDelay = statRow.isNullAt(0) ? null : statRow.getDouble(0);
            Integer minDelay = statRow.isNullAt(1) ? null : ((Number) statRow.get(1)).intValue();
            Integer maxDelay = statRow.isNullAt(2) ? null : ((Number) statRow.get(2)).intValue();

            Dataset<Row> topDelayed = joined
                    .orderBy(
                            desc("Delivery_Delay_Days"),
                            asc("Order_ID"),
                            asc("Product_ID"),
                            asc("Seller_ID")
                    )
                    .limit(20);

            List<String> lines = new ArrayList<>();
            lines.add("Metric|Value");
            lines.add("Total_Order_Items|" + totalItems);
            lines.add("Average_Delay_Days|" + (avgDelay == null ? "NULL" : String.format(java.util.Locale.US, "%.4f", avgDelay)));
            lines.add("Min_Delay_Days|" + (minDelay == null ? "NULL" : minDelay));
            lines.add("Max_Delay_Days|" + (maxDelay == null ? "NULL" : maxDelay));
            lines.add("Late_Items|" + lateCount);
            lines.add("OnTime_Items|" + onTimeCount);
            lines.add("Early_Items|" + earlyCount);
            lines.add("");
            lines.add("Order_ID|Product_ID|Seller_ID|Shipping_Limit_Date|Order_Delivered_Carrier_Date|Delivery_Delay_Days");

            for (Row r : topDelayed.collectAsList()) {
                lines.add(
                        r.getAs("Order_ID") + "|" +
                        r.getAs("Product_ID") + "|" +
                        r.getAs("Seller_ID") + "|" +
                        r.getAs("Shipping_Limit_Date") + "|" +
                        r.getAs("Order_Delivered_Carrier_Date") + "|" +
                        r.getAs("Delivery_Delay_Days")
                );
            }

            System.out.println("Writing output to: " + outPath);
            OutputWriter.writeLines(outPath, lines);
            System.out.println("Task 8 wrote " + lines.size() + " lines to " + outPath);
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