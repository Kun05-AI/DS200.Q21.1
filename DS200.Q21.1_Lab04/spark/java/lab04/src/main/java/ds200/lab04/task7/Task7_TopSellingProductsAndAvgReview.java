package ds200.lab04.task7;

import ds200.lab04.util.OutputWriter;
import ds200.lab04.util.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public final class Task7_TopSellingProductsAndAvgReview {
    private Task7_TopSellingProductsAndAvgReview() {}

    public static void main(String[] args) {
        if (args == null || args.length < 4) {
            System.err.println("Usage: Task7_TopSellingProductsAndAvgReview <order_items.csv> <order_reviews.csv> <products.csv> <out>");
            System.exit(1);
        }

        String itemsPath = args[0];
        String reviewsPath = args[1];
        String productsPath = args[2];
        String outPath = args[3];

        SparkSession spark = SparkSessionFactory.create("Task7_TopSellingProductsAndAvgReview");

        try {
            Dataset<Row> items = readCsv(spark, itemsPath);
            Dataset<Row> reviews = readCsv(spark, reviewsPath);
            Dataset<Row> products = readCsv(spark, productsPath);

            // 1. Làm sạch điểm review
            Dataset<Row> cleanReviews = reviews.withColumn(
                    "Review_Score_Clean",
                    when(trim(col("Review_Score")).rlike("^[1-5]$"), trim(col("Review_Score")).cast("int"))
                            .otherwise(lit(null).cast("int"))
            );

            // 2. Gom nhóm review theo Order_ID để tránh nhân bản dòng khi Join
            Dataset<Row> aggReviews = cleanReviews
                    .filter(col("Review_Score_Clean").isNotNull())
                    .groupBy("Order_ID")
                    .agg(avg("Review_Score_Clean").alias("Order_Avg_Review"));

            // 3. Tính tổng tiền và số lượng của từng Product_ID trong mỗi Order_ID
            Dataset<Row> orderProduct = items
                    .select("Order_ID", "Product_ID", "Price", "Freight_Value")
                    .groupBy("Order_ID", "Product_ID")
                    .agg(
                            sum(coalesce(col("Price"), lit(0.0))).alias("Price_Sum"),
                            sum(coalesce(col("Freight_Value"), lit(0.0))).alias("Freight_Sum"),
                            count(lit(1)).alias("Quantity_Sold")
                    );

            // 4. Join tất cả lại
            Dataset<Row> joined = orderProduct
                    .join(aggReviews, "Order_ID", "left")
                    .join(products.select("Product_ID", "Product_Category_Name"), "Product_ID", "left")
                    .withColumn("Revenue", col("Price_Sum").plus(col("Freight_Sum")));

            // 5. Gom nhóm cuối cùng theo Product_ID
            Dataset<Row> result = joined
                    .groupBy("Product_ID", "Product_Category_Name")
                    .agg(
                            sum("Quantity_Sold").alias("Total_Quantity_Sold"),
                            sum("Revenue").alias("Total_Revenue"),
                            avg("Order_Avg_Review").alias("Avg_Review_Score")
                    )
                    .orderBy(desc("Total_Quantity_Sold"), desc("Total_Revenue"), asc("Product_ID"))
                    .limit(20);

            List<String> lines = new ArrayList<>();
            lines.add("Product_ID|Product_Category_Name|Total_Quantity_Sold|Total_Revenue|Avg_Review_Score");

            for (Row r : result.collectAsList()) {
                Object avg = r.getAs("Avg_Review_Score");
                String avgStr = avg == null ? "NULL" : String.format(java.util.Locale.US, "%.4f", ((Number) avg).doubleValue());
                lines.add(
                        r.getAs("Product_ID") + "|" +
                        r.getAs("Product_Category_Name") + "|" +
                        r.getAs("Total_Quantity_Sold") + "|" +
                        String.format(java.util.Locale.US, "%.2f", ((Number) r.getAs("Total_Revenue")).doubleValue()) + "|" +
                        avgStr
                );
            }

            OutputWriter.writeLines(outPath, lines);
            System.out.println("Task 7 wrote " + lines.size() + " lines to " + outPath);
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