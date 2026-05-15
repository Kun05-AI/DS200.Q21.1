package ds200.lab03.task2;

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

public final class Task2App {
  private Task2App() {}

  private static final class GenreStat {
    private final String genre;
    private final double avg;
    private final long count;

    private GenreStat(String genre, double avg, long count) {
      this.genre = genre;
      this.avg = avg;
      this.count = count;
    }
  }

  public static void main(String[] args) throws Exception {
    if (args == null || args.length < 4) {
      System.err.println("Usage: Task2App <movies> <ratings1> <ratings2> <out>");
      System.exit(2);
    }

    String moviesFile = args[0];
    String r1 = args[1];
    String r2 = args[2];
    String outPath = args[3];

    try (JavaSparkContext sc = SparkContexts.localContext("Task2_GenreStats")) {

      Map<Integer, String[]> movieGenres = sc.textFile(moviesFile)
          .mapToPair(line -> {
            if (line == null || line.trim().isEmpty()) return null;

            int firstComma = line.indexOf(',');
            int lastComma = line.lastIndexOf(',');
            if (firstComma < 0 || lastComma <= firstComma) return null;

            try {
              int movieId = Integer.parseInt(line.substring(0, firstComma).trim());
              String genresPart = line.substring(lastComma + 1).trim();
              String[] genres = genresPart.split("\\|");
              return new Tuple2<>(movieId, genres);
            } catch (Exception e) {
              return null;
            }
          })
          .filter(Objects::nonNull)
          .collectAsMap();

      Broadcast<Map<Integer, String[]>> movieGenresBc = sc.broadcast(movieGenres);

      JavaRDD<String> ratingsLines = sc.textFile(r1).union(sc.textFile(r2));

      JavaPairRDD<String, Tuple2<Double, Long>> genreStats = ratingsLines
          .flatMapToPair(line -> {
            List<Tuple2<String, Tuple2<Double, Long>>> out = new ArrayList<>();
            if (line == null || line.trim().isEmpty()) return out.iterator();

            String[] p = line.split(",", -1);
            if (p.length < 4) return out.iterator();

            try {
              int movieId = Integer.parseInt(p[1].trim());
              double rating = Double.parseDouble(p[2].trim());

              String[] genres = movieGenresBc.value().get(movieId);
              if (genres == null) return out.iterator();

              for (String g : genres) {
                String genre = g.trim();
                if (!genre.isEmpty()) {
                  out.add(new Tuple2<>(genre, new Tuple2<>(rating, 1L)));
                }
              }
            } catch (Exception ignored) {
            }

            return out.iterator();
          })
          .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

      List<GenreStat> rows = new ArrayList<>();
      for (Tuple2<String, Tuple2<Double, Long>> t : genreStats.collect()) {
        String genre = t._1;
        double sum = t._2._1;
        long count = t._2._2;
        rows.add(new GenreStat(genre, sum / count, count));
      }

      rows.sort(new Comparator<GenreStat>() {
        @Override
        public int compare(GenreStat a, GenreStat b) {
          int c = Double.compare(b.avg, a.avg);
          if (c != 0) return c;
          return a.genre.compareTo(b.genre);
        }
      });

      List<String> out = new ArrayList<>();
      out.add("Genre|AverageRating|TotalRatings");
      for (GenreStat row : rows) {
        out.add(
            row.genre + "|" +
            String.format(Locale.US, "%.4f", row.avg) + "|" +
            row.count
        );
      }

      OutputWriter.writeLines(outPath, out);
      System.out.println("Wrote " + out.size() + " lines to " + outPath);
    }
  }
}