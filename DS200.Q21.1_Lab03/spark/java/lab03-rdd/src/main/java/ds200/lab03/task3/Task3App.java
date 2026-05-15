package ds200.lab03.task3;

import ds200.lab03.util.OutputWriter;
import ds200.lab03.util.SparkContexts;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class Task3App {
  private Task3App() {}

  private static final class MovieGenderStat {
    private final int movieId;
    private final String title;
    private final String gender;
    private final double avg;
    private final long count;

    private MovieGenderStat(int movieId, String title, String gender, double avg, long count) {
      this.movieId = movieId;
      this.title = title;
      this.gender = gender;
      this.avg = avg;
      this.count = count;
    }
  }

  public static void main(String[] args) throws Exception {
    if (args == null || args.length < 5) {
      System.err.println("Usage: Task3App <movies> <users> <ratings1> <ratings2> <out>");
      System.exit(2);
    }

    String moviesFile = args[0];
    String usersFile = args[1];
    String r1 = args[2];
    String r2 = args[3];
    String outPath = args[4];

    try (JavaSparkContext sc = SparkContexts.localContext("Task3_GenderStats")) {

      // MovieID -> Title
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

      Broadcast<Map<Integer, String>> movieBc = sc.broadcast(movieById);

      // UserID -> Gender
      Map<Integer, String> userGender = sc.textFile(usersFile)
          .mapToPair(line -> {
            if (line == null || line.trim().isEmpty()) return null;

            String[] p = line.split(",", -1);
            if (p.length < 5) return null;

            try {
              int userId = Integer.parseInt(p[0].trim());
              String gender = p[1].trim();
              return new Tuple2<>(userId, gender);
            } catch (Exception e) {
              return null;
            }
          })
          .filter(Objects::nonNull)
          .collectAsMap();

      Broadcast<Map<Integer, String>> userGenderBc = sc.broadcast(userGender);

      JavaRDD<String> ratingsLines = sc.textFile(r1).union(sc.textFile(r2));

      // ((MovieID, Gender), (sumRating, count))
      JavaPairRDD<Tuple2<Integer, String>, Tuple2<Double, Long>> stats = ratingsLines
          .mapToPair(line -> {
            if (line == null || line.trim().isEmpty()) return null;

            String[] p = line.split(",", -1);
            if (p.length < 4) return null;

            try {
              int userId = Integer.parseInt(p[0].trim());
              int movieId = Integer.parseInt(p[1].trim());
              double rating = Double.parseDouble(p[2].trim());

              String gender = userGenderBc.value().get(userId);
              if (gender == null || gender.isEmpty()) return null;

              return new Tuple2<>(
                  new Tuple2<>(movieId, gender),
                  new Tuple2<>(rating, 1L)
              );
            } catch (Exception e) {
              return null;
            }
          })
          .filter(Objects::nonNull)
          .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

      List<MovieGenderStat> rows = new ArrayList<>();

      for (Tuple2<Tuple2<Integer, String>, Tuple2<Double, Long>> t : stats.collect()) {
        int movieId = t._1._1;
        String gender = t._1._2;
        double sum = t._2._1;
        long count = t._2._2;

        String title = movieBc.value().get(movieId);
        if (title == null) title = "<unknown>";

        rows.add(new MovieGenderStat(movieId, title, gender, sum / count, count));
      }

      rows.sort(new Comparator<MovieGenderStat>() {
        @Override
        public int compare(MovieGenderStat a, MovieGenderStat b) {
          int c = Integer.compare(a.movieId, b.movieId);
          if (c != 0) return c;
          return a.gender.compareTo(b.gender);
        }
      });

      List<String> out = new ArrayList<>();
      out.add("MovieID|Title|Gender|AverageRating|TotalRatings");

      for (MovieGenderStat row : rows) {
        out.add(
            row.movieId + "|" +
            row.title + "|" +
            row.gender + "|" +
            String.format(Locale.US, "%.4f", row.avg) + "|" +
            row.count
        );
      }

      OutputWriter.writeLines(outPath, out);
      System.out.println("Wrote " + out.size() + " lines to " + outPath);
    }
  }
}