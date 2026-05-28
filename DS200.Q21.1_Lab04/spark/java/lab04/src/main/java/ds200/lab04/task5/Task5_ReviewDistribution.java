package ds200.lab04.task5;

import ds200.lab04.util.OutputWriter;
import ds200.lab04.util.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public final class Task5_ReviewDistribution {
    private Task5_ReviewDistribution() {}

    public static void main(String[] args) {
        if (args == null || args.length < 2) {
            System.err.println("Usage: Task5_ReviewDistribution <order_reviews.csv> <out>");
            System.exit(1);
        }

        String reviewsPath = args[0];
        String outPath = args[1];

        SparkSession spark = SparkSessionFactory.create("Task5_ReviewDistribution");

        try {
            Dataset<Row> reviews = readCsv(spark, reviewsPath);

            Dataset<Row> cleaned = reviews.withColumn(
                    "Review_Score_Clean",
                    when(trim(col("Review_Score")).rlike("^[1-5]$"), trim(col("Review_Score")).cast("int"))
                            .otherwise(lit(null).cast("int"))
            );

            long totalRows = cleaned.count();
            long nullRows = cleaned.filter(col("Review_Score").isNull()).count();
            long invalidRows = cleaned.filter(col("Review_Score").isNotNull()
                    .and(not(trim(col("Review_Score")).rlike("^[1-5]$")))).count();
            long validRows = cleaned.filter(col("Review_Score_Clean").isNotNull()).count();

            Row avgRow = cleaned.agg(avg("Review_Score_Clean").alias("avg")).first();
            Double avgScore = avgRow.isNullAt(0) ? null : avgRow.getDouble(0);

            Dataset<Row> dist = cleaned
                    .filter(col("Review_Score_Clean").isNotNull())
                    .groupBy("Review_Score_Clean")
                    .agg(count(lit(1)).alias("Review_Count"))
                    .orderBy(asc("Review_Score_Clean"));

            List<String> lines = new ArrayList<>();
            lines.add("Metric|Value");
            lines.add("Total_Review_Rows|" + totalRows);
            lines.add("Valid_Review_Rows|" + validRows);
            lines.add("Null_Review_Rows|" + nullRows);
            lines.add("Invalid_Review_Rows|" + invalidRows);
            lines.add("Average_Review_Score|" + (avgScore == null ? "NULL" : String.format(java.util.Locale.US, "%.4f", avgScore)));
            lines.add("");
            lines.add("Review_Score|Count");

            for (Row r : dist.collectAsList()) {
                lines.add(r.getAs("Review_Score_Clean") + "|" + r.getAs("Review_Count"));
            }

            OutputWriter.writeLines(outPath, lines);
            System.out.println("Task 5 wrote " + lines.size() + " lines to " + outPath);
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