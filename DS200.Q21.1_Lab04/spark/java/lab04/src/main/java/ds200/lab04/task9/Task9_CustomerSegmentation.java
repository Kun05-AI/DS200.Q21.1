package ds200.lab04.task9;

import ds200.lab04.util.OutputWriter;
import ds200.lab04.util.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public final class Task9_CustomerSegmentation {

    private Task9_CustomerSegmentation() {}

    public static void main(String[] args) {

        /*
         * =========================================================
         * Arguments
         * =========================================================
         */

        if (args == null || args.length < 4) {

            System.err.println(
                    "Usage: Task9_CustomerSegmentation " +
                    "<orders.csv> " +
                    "<customer_list.csv> " +
                    "<order_items.csv> " +
                    "<output.txt>"
            );

            System.exit(1);
        }

        String ordersPath = args[0];
        String customersPath = args[1];
        String itemsPath = args[2];
        String outPath = args[3];

        /*
         * =========================================================
         * Spark Session
         * =========================================================
         */

        SparkSession spark =
                SparkSessionFactory.create(
                        "Task9_CustomerSegmentation"
                );

        spark.conf().set("spark.sql.shuffle.partitions", "4");

        try {

            /*
             * =========================================================
             * Read CSV
             * =========================================================
             */

            Dataset<Row> orders =
                    readCsv(spark, ordersPath);

            Dataset<Row> customers =
                    readCsv(spark, customersPath)
                            .dropDuplicates("Customer_Trx_ID");

            Dataset<Row> items =
                    readCsv(spark, itemsPath);

            /*
             * =========================================================
             * Calculate Order Value
             * =========================================================
             */

            Dataset<Row> orderValues = items

                    .withColumn(
                            "Item_Value",

                            coalesce(
                                    col("Price"),
                                    lit(0.0)
                            )

                            .plus(

                                    coalesce(
                                            col("Freight_Value"),
                                            lit(0.0)
                                    )
                            )
                    )

                    .groupBy("Order_ID")

                    .agg(

                            round(
                                    sum("Item_Value"),
                                    2
                            )

                            .alias("Order_Value")
                    );

            /*
             * =========================================================
             * Base Dataset
             * =========================================================
             */

            Dataset<Row> base = orders

                    .select(
                            col("Order_ID"),
                            col("Customer_Trx_ID"),
                            col("Order_Purchase_Timestamp")
                    )

                    .join(
                            orderValues,
                            "Order_ID",
                            "left"
                    )

                    .join(

                            customers.select(

                                    col("Customer_Trx_ID"),
                                    col("Subscriber_ID"),
                                    col("Customer_Country"),
                                    col("Customer_City"),
                                    col("Age"),
                                    col("Gender")
                            ),

                            "Customer_Trx_ID",
                            "left"
                    )

                    .filter(
                            col("Subscriber_ID").isNotNull()
                    );

            /*
             * =========================================================
             * Customer Metrics
             * =========================================================
             */

            Dataset<Row> perCustomer = base

                    .groupBy(

                            "Subscriber_ID",
                            "Customer_Country",
                            "Customer_City",
                            "Age",
                            "Gender"
                    )

                    .agg(

                            countDistinct("Order_ID")
                                    .alias("Total_Orders"),

                            round(
                                    sum(
                                            coalesce(
                                                    col("Order_Value"),
                                                    lit(0.0)
                                            )
                                    ),
                                    2
                            )

                            .alias("Total_Spend"),

                            round(
                                    avg(
                                            coalesce(
                                                    col("Order_Value"),
                                                    lit(0.0)
                                            )
                                    ),
                                    2
                            )

                            .alias("Avg_Order_Value"),

                            min("Order_Purchase_Timestamp")
                                    .alias("First_Order_Ts"),

                            max("Order_Purchase_Timestamp")
                                    .alias("Last_Order_Ts")
                    )

                    /*
                     * =========================================================
                     * Active Days
                     * =========================================================
                     */

                    .withColumn(

                            "Active_Days",

                            greatest(

                                    datediff(

                                            to_date(
                                                    substring(
                                                            col("Last_Order_Ts")
                                                                    .cast("string"),
                                                            1,
                                                            10
                                                    )
                                            ),

                                            to_date(
                                                    substring(
                                                            col("First_Order_Ts")
                                                                    .cast("string"),
                                                            1,
                                                            10
                                                    )
                                            )
                                    ).plus(lit(1)),

                                    lit(1)
                            )
                    )

                    /*
                     * =========================================================
                     * Frequency Per Month
                     * =========================================================
                     */

                    .withColumn(

                        "Frequency_Per_Month",

                        when(

                                col("Active_Days").leq(30),

                                col("Total_Orders")
                                        .cast("double")
                        )

                        .otherwise(

                                round(

                                        col("Total_Orders")
                                                .cast("double")

                                                .divide(

                                                        col("Active_Days")
                                                                .cast("double")

                                                                .divide(lit(30.0))
                                                ),

                                         4
                                  )
                         )
                )
                    /*
                     * =========================================================
                     * Customer Segmentation
                     * =========================================================
                     */

                    .withColumn(

                            "Segment",

                            /*
                             * New Customer
                             */

                            when(
                                    col("Total_Orders").equalTo(1),
                                    lit("New")
                            )

                            /*
                             * Loyal Customer
                             */

                            .when(

                                    col("Total_Orders").geq(4)

                                            .and(
                                                    col("Avg_Order_Value")
                                                            .geq(500.0)
                                            )

                                            .and(
                                                    col("Frequency_Per_Month")
                                                            .geq(2.0)
                                            ),

                                    lit("Loyal")
                            )

                            /*
                             * Regular Customer
                             */

                            .when(

                                    col("Total_Orders").between(2, 3),

                                    lit("Regular")
                            )

                            /*
                             * Occasional Customer
                             */

                            .otherwise(
                                    lit("Occasional")
                            )
                    );

            /*
             * =========================================================
             * Segment Summary
             * =========================================================
             */

            Dataset<Row> segmentSummary = perCustomer

                    .groupBy("Segment")

                    .agg(

                            count(lit(1))
                                    .alias("Customer_Count"),

                            round(
                                    avg("Total_Orders"),
                                    4
                            )

                            .alias("Avg_Total_Orders"),

                            round(
                                    avg("Total_Spend"),
                                    2
                            )

                            .alias("Avg_Total_Spend"),

                            round(
                                    avg("Avg_Order_Value"),
                                    2
                            )

                            .alias("Avg_Order_Value"),

                            round(
                                    avg("Frequency_Per_Month"),
                                    4
                            )

                            .alias("Avg_Frequency_Per_Month")
                    )

                    /*
                     * Sort Segment
                     */

                    .withColumn(

                            "SortKey",

                            when(
                                    col("Segment").equalTo("New"),
                                    lit(1)
                            )

                            .when(
                                    col("Segment").equalTo("Regular"),
                                    lit(2)
                            )

                            .when(
                                    col("Segment").equalTo("Loyal"),
                                    lit(3)
                            )

                            .otherwise(lit(4))
                    )

                    .orderBy(col("SortKey"))

                    .drop("SortKey");

            /*
             * =========================================================
             * Top Customers
             * =========================================================
             */

            Dataset<Row> topCustomers = perCustomer

                    .orderBy(

                            desc("Total_Spend"),

                            desc("Total_Orders"),

                            asc("Subscriber_ID")
                    )

                    .limit(20);

            /*
             * =========================================================
             * Output
             * =========================================================
             */

            List<String> lines = new ArrayList<>();

            /*
             * =========================================================
             * Segment Summary Output
             * =========================================================
             */

            lines.add("SEGMENT_SUMMARY");

            lines.add(
                    "Segment|" +
                    "Customer_Count|" +
                    "Avg_Total_Orders|" +
                    "Avg_Total_Spend|" +
                    "Avg_Order_Value|" +
                    "Avg_Frequency_Per_Month"
            );

            Iterator<Row> summaryIterator =
                    segmentSummary.toLocalIterator();

            while (summaryIterator.hasNext()) {

                Row r = summaryIterator.next();

                lines.add(

                        r.getAs("Segment") + "|" +

                        r.getAs("Customer_Count") + "|" +

                        String.format(
                                java.util.Locale.US,
                                "%.4f",
                                ((Number)
                                        r.getAs("Avg_Total_Orders"))
                                        .doubleValue()
                        ) + "|" +

                        String.format(
                                java.util.Locale.US,
                                "%.2f",
                                ((Number)
                                        r.getAs("Avg_Total_Spend"))
                                        .doubleValue()
                        ) + "|" +

                        String.format(
                                java.util.Locale.US,
                                "%.2f",
                                ((Number)
                                        r.getAs("Avg_Order_Value"))
                                        .doubleValue()
                        ) + "|" +

                        String.format(
                                java.util.Locale.US,
                                "%.4f",
                                ((Number)
                                        r.getAs(
                                                "Avg_Frequency_Per_Month"
                                        ))
                                        .doubleValue()
                        )
                );
            }

            lines.add("");

            /*
             * =========================================================
             * Top Customers Output
             * =========================================================
             */

            lines.add("TOP_CUSTOMERS");

            lines.add(
                    "Subscriber_ID|" +
                    "Customer_Country|" +
                    "Customer_City|" +
                    "Age|" +
                    "Gender|" +
                    "Total_Orders|" +
                    "Total_Spend|" +
                    "Avg_Order_Value|" +
                    "Frequency_Per_Month|" +
                    "Segment"
            );

            Iterator<Row> topIterator =
                    topCustomers.toLocalIterator();

            while (topIterator.hasNext()) {

                Row r = topIterator.next();

                lines.add(

                        r.getAs("Subscriber_ID") + "|" +

                        r.getAs("Customer_Country") + "|" +

                        r.getAs("Customer_City") + "|" +

                        r.getAs("Age") + "|" +

                        r.getAs("Gender") + "|" +

                        r.getAs("Total_Orders") + "|" +

                        String.format(
                                java.util.Locale.US,
                                "%.2f",
                                ((Number)
                                        r.getAs("Total_Spend"))
                                        .doubleValue()
                        ) + "|" +

                        String.format(
                                java.util.Locale.US,
                                "%.2f",
                                ((Number)
                                        r.getAs("Avg_Order_Value"))
                                        .doubleValue()
                        ) + "|" +

                        String.format(
                                java.util.Locale.US,
                                "%.4f",
                                ((Number)
                                        r.getAs(
                                                "Frequency_Per_Month"
                                        ))
                                        .doubleValue()
                        ) + "|" +

                        r.getAs("Segment")
                );
            }

            /*
             * =========================================================
             * Write Output
             * =========================================================
             */

            OutputWriter.writeLines(outPath, lines);

            System.out.println(
                    "Task 9 completed successfully."
            );

            System.out.println(
                    "Output written to: " + outPath
            );

        } finally {

            spark.stop();
        }
    }

    /*
     * =========================================================
     * CSV Reader
     * =========================================================
     */

    private static Dataset<Row> readCsv(
            SparkSession spark,
            String path
    ) {

        return spark.read()

                .option("header", "true")

                .option("inferSchema", "true")

                .option("sep", ";")

                .option("encoding", "UTF-8")

                .option("mode", "PERMISSIVE")

                .csv(path);
    }
}