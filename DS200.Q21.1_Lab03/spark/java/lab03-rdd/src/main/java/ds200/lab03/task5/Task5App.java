package ds200.lab03.task5;

import ds200.lab03.util.OutputWriter;
import ds200.lab03.util.SparkContexts;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class Task5App {
  private Task5App() {}

  public static void main(String[] args) throws Exception {
    if (args == null || args.length < 4) {
      System.err.println("Usage: Task5App <movies> <ratings1> <ratings2> <out>");
      System.exit(2);
    }

    String moviesFile = args[0];
    String r1 = args[1];
    String r2 = args[2];
    String outPath = args[3];

    try (JavaSparkContext sc = SparkContexts.localContext("Task5_RatingDist")) {

      // MovieID -> Title
      Map<Integer, String> movieById = sc.textFile(moviesFile)
          .mapToPair(line -> {
            int firstComma = line.indexOf(',');
            int lastComma = line.lastIndexOf(',');

            if (firstComma < 0 || lastComma <= firstComma) {
              return null;
            }

            try {
              int movieId = Integer.parseInt(line.substring(0, firstComma).trim());
              String title = line.substring(firstComma + 1, lastComma).trim();
              return new Tuple2<>(movieId, title);
            } catch (Exception e) {
              return null;
            }
          })
          .filter(Objects::nonNull)
          .collectAsMap();

      JavaRDD<String> ratingsLines = sc.textFile(r1).union(sc.textFile(r2));

      // (MovieID|RatingValue) -> count
      JavaPairRDD<String, Long> counted = ratingsLines
          .mapToPair(line -> {
            String[] p = line.split(",", -1);
            if (p.length < 4) return null;

            try {
              int movieId = Integer.parseInt(p[1].trim());
              double rating = Double.parseDouble(p[2].trim());

              String key = movieId + "|" + String.format(Locale.US, "%.4f", rating);
              return new Tuple2<>(key, 1L);
            } catch (Exception e) {
              return null;
            }
          })
          .filter(Objects::nonNull)
          .reduceByKey(Long::sum);

      List<Tuple2<String, Long>> rows = new ArrayList<>(counted.collect());

      // Sort by MovieID asc, RatingValue desc
      rows.sort(new Comparator<Tuple2<String, Long>>() {
        @Override
        public int compare(Tuple2<String, Long> a, Tuple2<String, Long> b) {
          String[] pa = a._1.split("\\|", 2);
          String[] pb = b._1.split("\\|", 2);

          int midA = Integer.parseInt(pa[0]);
          int midB = Integer.parseInt(pb[0]);
          if (midA != midB) return Integer.compare(midA, midB);

          double ra = Double.parseDouble(pa[1]);
          double rb = Double.parseDouble(pb[1]);
          return Double.compare(rb, ra);
        }
      });

      List<String> out = new ArrayList<>();
      out.add("MovieID|Title|RatingValue|Count");

      for (Tuple2<String, Long> t : rows) {
        String[] parts = t._1.split("\\|", 2);
        int movieId = Integer.parseInt(parts[0]);
        String ratingVal = parts.length > 1 ? parts[1] : "";
        String title = movieById.get(movieId);
        if (title == null) title = "<unknown>";

        out.add(movieId + "|" + title + "|" + ratingVal + "|" + t._2);
      }

      OutputWriter.writeLines(outPath, out);
      System.out.println("Wrote " + out.size() + " lines to " + outPath);
    }
  }
}