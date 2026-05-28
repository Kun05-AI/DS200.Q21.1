package ds200.lab04.task4;

import ds200.lab04.util.OutputWriter;
import ds200.lab04.util.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public final class Task4_OrdersByYearMonth {
    private Task4_OrdersByYearMonth() {}

    public static void main(String[] args) {
        // Kiểm tra tham số đầu vào
        if (args == null || args.length < 2) {
            System.err.println("Usage: Task4_OrdersByYearMonth <orders.csv> <out>");
            System.exit(1);
        }

        String ordersPath = args[0];
        String outPath = args[1];

        
        SparkSession spark = SparkSessionFactory.create("Task4_OrdersByYearMonth");

        try {
            // Đọc file CSV
            Dataset<Row> orders = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option("sep", ";")
                    .option("encoding", "UTF-8")
                    .csv(ordersPath)
                    .withColumn("Order_Purchase_Timestamp", to_timestamp(col("Order_Purchase_Timestamp")));

            //Group theo năm tháng và đếm order
            Dataset<Row> result = orders
                    .withColumn("Year", year(col("Order_Purchase_Timestamp")))
                    .withColumn("Month", month(col("Order_Purchase_Timestamp")))
                    .groupBy("Year", "Month")
                    .agg(countDistinct("Order_ID").alias("Order_Count"))
                    .orderBy(asc("Year"), desc("Month")); // Sắp xếp xuôi dòng thời gian

            
            List<String> lines = new ArrayList<>();
            lines.add("Year|Month|Order_Count");

            for (Row r : result.collectAsList()) {
                // Format tháng thành 01, 02...
                String formattedMonth = String.format("%02d", (int) r.getAs("Month"));
                lines.add(r.getAs("Year") + "|" + formattedMonth + "|" + r.getAs("Order_Count"));
            }

            OutputWriter.writeLines(outPath, lines);
            System.out.println("SUCCESS: Task 4 đã ghi dữ liệu vào " + outPath);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}