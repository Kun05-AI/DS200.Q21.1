package ds200.lab03.task6;

import ds200.lab03.model.RatingStats;
import ds200.lab03.util.Lab03Parse;
import ds200.lab03.util.OutputWriter;
import ds200.lab03.util.SparkContexts;
import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class Task6App {
  private Task6App() {}

  public static void main(String[] args) throws Exception {
    if (args == null || args.length < 4) {
      System.err.println("Usage: Task6App <movies> <ratings1> <ratings2> <out>");
      System.exit(2);
    }

    String r1 = args[1];
    String r2 = args[2];
    String outPath = args[3];

    try (JavaSparkContext sc = SparkContexts.localContext("Task6_RatingsByYear")) {
      JavaRDD<String> ratingsLines = sc.textFile(r1).union(sc.textFile(r2));
      JavaRDD<Object> ratingsParsed = ratingsLines
          .map(line -> (Object) Lab03Parse.parseRating(line))
          .filter(Objects::nonNull);

      List<Tuple2<Integer, RatingStats>> byYear = new ArrayList<>(
          ratingsParsed
              .mapToPair(r -> {
                long ts = getLongFromObject(r, "timestamp", "time", "getTimestamp", "getTime");
                int year = Instant.ofEpochSecond(ts).atZone(ZoneId.of("UTC")).getYear();
                return new Tuple2<>(year, new RatingStats(getDoubleFromObject(r, "score", "rating", "getScore", "getRating"), 1L));
              })
              .reduceByKey(RatingStats::combine)
              .collect()
      );

      List<String> out = new ArrayList<>();
      out.add("Year|AverageRating|TotalRatings");
      byYear.sort((a, b) -> Integer.compare(a._1, b._1));
      for (var t : byYear) {
        out.add(t._1 + "|" + Lab03Parse.fmt(t._2.calcAvg()) + "|" + t._2.recordCount());
      }

      OutputWriter.writeLines(outPath, out);
      System.out.println("Wrote " + out.size() + " lines to " + outPath);
    }
  }

  private static long getLongFromObject(Object o, String... names) {
    Object v = callAny(o, names);
    if (v == null) return 0L;
    if (v instanceof Number) return ((Number) v).longValue();
    try { return Long.parseLong(String.valueOf(v)); } catch (Exception e) { return 0L; }
  }

  private static double getDoubleFromObject(Object o, String... names) {
    Object v = callAny(o, names);
    if (v == null) return 0.0;
    if (v instanceof Number) return ((Number) v).doubleValue();
    try { return Double.parseDouble(String.valueOf(v)); } catch (Exception e) { return 0.0; }
  }

  private static Object callAny(Object o, String... names) {
    if (o == null) return null;
    Class<?> c = o.getClass();
    for (String n : names) {
      try { Method m = c.getMethod(n); return m.invoke(o); } catch (NoSuchMethodException ignored) {} catch (IllegalAccessException | InvocationTargetException ignored) {}
      try { Method m = c.getMethod("get" + capitalize(n)); return m.invoke(o); } catch (NoSuchMethodException ignored) {} catch (IllegalAccessException | InvocationTargetException ignored) {}
    }
    return null;
  }

  private static String capitalize(String s) {
    if (s == null || s.isEmpty()) return s;
    return Character.toUpperCase(s.charAt(0)) + s.substring(1);
  }
}
