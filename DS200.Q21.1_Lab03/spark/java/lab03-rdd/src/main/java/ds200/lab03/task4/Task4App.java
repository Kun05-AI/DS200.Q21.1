package ds200.lab03.task4;

import ds200.lab03.util.OutputWriter;
import ds200.lab03.util.SparkContexts;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class Task4App {
  private Task4App() {}

  public static void main(String[] args) throws Exception {
    if (args == null || args.length < 5) {
      System.err.println("Usage: Task4App <users> <ratings1> <ratings2> <out> <minRatings> [movies]");
      System.exit(2);
    }

    String usersFile = args[0];
    String r1 = args[1];
    String r2 = args[2];
    String outPath = args[3];
    int minRatings = Integer.parseInt(args[4]);
    String moviesFile = args.length >= 6 ? args[5] : null;

    try (JavaSparkContext sc = SparkContexts.localContext("Task4_AgeGroupSummary")) {

      // users.txt -> UserID -> AgeGroup
      JavaPairRDD<Integer, String> userAgeGroupRDD = sc.textFile(usersFile)
          .mapToPair(line -> {
            String[] p = line.split(",", -1);
            if (p.length < 5) return null;

            try {
              int uid = Integer.parseInt(p[0].trim());
              int age = Integer.parseInt(p[2].trim());
              return new Tuple2<>(uid, ageGroup(age));
            } catch (Exception e) {
              return null;
            }
          })
          .filter(t -> t != null);

      final Map<Integer, String> userAgeGroup = userAgeGroupRDD.collectAsMap();
      final Broadcast<Map<Integer, String>> userAgeBc = sc.broadcast(userAgeGroup);

      // movies.txt -> MovieID -> Title (optional)
      Map<Integer, String> movieTitlesTmp = new HashMap<>();
      if (moviesFile != null) {
        movieTitlesTmp = sc.textFile(moviesFile)
            .mapToPair(line -> {
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
            .filter(t -> t != null)
            .collectAsMap();
      }
      final Map<Integer, String> movieTitles = movieTitlesTmp;
      final Broadcast<Map<Integer, String>> movieBc = sc.broadcast(movieTitles);

      // ratings_1 + ratings_2
      JavaRDD<String> ratingsLines = sc.textFile(r1).union(sc.textFile(r2));

      // ((MovieID, AgeGroup), (rating, 1))
      JavaPairRDD<Tuple2<Integer, String>, Tuple2<Double, Long>> mapped = ratingsLines
          .mapToPair(line -> {
            String[] p = line.split(",", -1);
            if (p.length < 4) return null;

            try {
              int uid = Integer.parseInt(p[0].trim());
              int movieId = Integer.parseInt(p[1].trim());
              double rating = Double.parseDouble(p[2].trim());

              String group = userAgeBc.value().get(uid);
              if (group == null) return null;

              return new Tuple2<>(
                  new Tuple2<>(movieId, group),
                  new Tuple2<>(rating, 1L)
              );
            } catch (Exception e) {
              return null;
            }
          })
          .filter(t -> t != null);

      JavaPairRDD<Tuple2<Integer, String>, Tuple2<Double, Long>> reduced =
          mapped.reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

      List<Tuple2<Tuple2<Integer, String>, Tuple2<Double, Long>>> rows =
          new ArrayList<>(reduced.collect());

      rows.sort(new Comparator<Tuple2<Tuple2<Integer, String>, Tuple2<Double, Long>>>() {
        @Override
        public int compare(Tuple2<Tuple2<Integer, String>, Tuple2<Double, Long>> a,
                           Tuple2<Tuple2<Integer, String>, Tuple2<Double, Long>> b) {
          int c = Integer.compare(a._1._1, b._1._1);
          if (c != 0) return c;
          return a._1._2.compareTo(b._1._2);
        }
      });

      List<String> out = new ArrayList<>();
      if (moviesFile != null) {
        out.add("MovieID|Title|AgeGroup|AverageRating|TotalRatings");
      } else {
        out.add("MovieID|AgeGroup|AverageRating|TotalRatings");
      }

      for (Tuple2<Tuple2<Integer, String>, Tuple2<Double, Long>> row : rows) {
        long count = row._2._2;
        if (count < minRatings) continue;

        int movieId = row._1._1;
        String group = row._1._2;
        double avg = row._2._1 / count;

        if (moviesFile != null) {
          String title = movieBc.value().get(movieId);
          if (title == null) title = String.valueOf(movieId);

          out.add(
              movieId + "|" +
              title + "|" +
              group + "|" +
              String.format(Locale.US, "%.2f", avg) + "|" +
              count
          );
        } else {
          out.add(
              movieId + "|" +
              group + "|" +
              String.format(Locale.US, "%.2f", avg) + "|" +
              count
          );
        }
      }

      OutputWriter.writeLines(outPath, out);
      System.out.println("Wrote " + out.size() + " lines to " + outPath);
    }
  }

  private static String ageGroup(int age) {
    if (age < 18) return "0-17";
    if (age <= 24) return "18-24";
    if (age <= 34) return "25-34";
    if (age <= 44) return "35-44";
    if (age <= 49) return "45-49";
    if (age <= 55) return "50-55";
    return "56+";
  }
}