package ds200.lab03.task1;

import ds200.lab03.util.OutputWriter;
import ds200.lab03.util.SparkContexts;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class Task1App {
  private Task1App() {}

  private static final class MovieStat {
    private final int movieId;
    private final String title;
    private final double avg;
    private final long count;

    private MovieStat(int movieId, String title, double avg, long count) {
      this.movieId = movieId;
      this.title = title;
      this.avg = avg;
      this.count = count;
    }
  }

  public static void main(String[] args) throws Exception {
    if (args == null || args.length < 4) {
      System.err.println("Usage: Task1App <movies> <ratings1> <ratings2> <out> [minRatings]");
      System.exit(2);
    }

    String moviesFile = args[0];
    String r1 = args[1];
    String r2 = args[2];
    String outPath = args[3];
    int minRatings = args.length >= 5 ? Integer.parseInt(args[4]) : 50;

    try (JavaSparkContext sc = SparkContexts.localContext("Task1_MovieStats")) {

      Map<Integer, String> movieById = sc.textFile(moviesFile)
          .mapToPair(line -> {
            if (line == null || line.trim().isEmpty()) return null;

            int firstComma = line.indexOf(',');
            int lastComma = line.lastIndexOf(',');
            if (firstComma < 0 || lastComma <= firstComma) return null;

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

      JavaPairRDD<Integer, Tuple2<Double, Long>> movieStats = ratingsLines
          .mapToPair(line -> {
            if (line == null || line.trim().isEmpty()) return null;

            String[] p = line.split(",", -1);
            if (p.length < 4) return null;

            try {
              int movieId = Integer.parseInt(p[1].trim());
              double rating = Double.parseDouble(p[2].trim());
              return new Tuple2<>(movieId, new Tuple2<>(rating, 1L));
            } catch (Exception e) {
              return null;
            }
          })
          .filter(Objects::nonNull)
          .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

      List<MovieStat> rows = new ArrayList<>();
      for (Tuple2<Integer, Tuple2<Double, Long>> t : movieStats.collect()) {
        int movieId = t._1;
        double sum = t._2._1;
        long count = t._2._2;
        String title = movieById.get(movieId);
        if (title == null) title = "<unknown>";
        rows.add(new MovieStat(movieId, title, sum / count, count));
      }

      rows.sort(new Comparator<MovieStat>() {
        @Override
        public int compare(MovieStat a, MovieStat b) {
          return Integer.compare(a.movieId, b.movieId);
        }
      });

      MovieStat top = null;
      for (MovieStat row : rows) {
        if (row.count < minRatings) continue;
        if (top == null
            || row.avg > top.avg
            || (row.avg == top.avg && row.count > top.count)
            || (row.avg == top.avg && row.count == top.count && row.movieId < top.movieId)) {
          top = row;
        }
      }

      List<String> out = new ArrayList<>();
      out.add("MovieID|Title|AverageRating|TotalRatings");
      for (MovieStat row : rows) {
        out.add(
            row.movieId + "|" +
            row.title + "|" +
            String.format(Locale.US, "%.4f", row.avg) + "|" +
            row.count
        );
      }

      if (top == null) {
        out.add("TOP_MOVIE(minRatings=" + minRatings + ")|<none>");
      } else {
        out.add(
            "TOP_MOVIE(minRatings=" + minRatings + ")|" +
            top.movieId + "|" +
            top.title + "|" +
            String.format(Locale.US, "%.4f", top.avg) + "|" +
            top.count
        );
      }

      OutputWriter.writeLines(outPath, out);
      System.out.println("Wrote " + out.size() + " lines to " + outPath);
    }
  }
}