package ds200.lab03.util;
import ds200.lab03.model.*;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.AbstractMap;
import java.util.Map;

public final class Lab03Parse {
    public static Movie parseMovie(String line) {
        try {
            var parts = line.split(",", 3);
            int id = Integer.parseInt(parts[0]);
            String title = parts[1];
            var genres = Arrays.asList(parts[2].split("\\|"));
            return new Movie(id, title, genres);
        } catch (Exception e) { return null; }
    }

    public static Rating parseRating(String line) {
        try {
            var p = line.split(",");
            return new Rating(Integer.parseInt(p[0]), Integer.parseInt(p[1]), Double.parseDouble(p[2]), Long.parseLong(p[3]));
        } catch (Exception e) { return null; }
    }

    public static User parseUser(String line) {
        try {
            var p = line.split(",");
            return new User(Integer.parseInt(p[0]), p[1], Integer.parseInt(p[2]), Integer.parseInt(p[3]));
        } catch (Exception e) { return null; }
    }

    public static Map.Entry<Integer, String> parseOccupation(String line) {
        try {
            var p = line.split(",");
            return new AbstractMap.SimpleEntry<>(Integer.parseInt(p[0]), p[1]);
        } catch (Exception e) { return null; }
    }

    public static String ageGroup(int age) {
        if (age <= 18) return "0-18";
        if (age <= 35) return "19-35";
        if (age <= 50) return "36-50";
        return "51+";
    }

    public static int yearFromTimestamp(long ts) {
        return Instant.ofEpochSecond(ts).atZone(ZoneId.of("UTC")).getYear();
    }

    public static String fmt(double val) {
        return String.format(java.util.Locale.US, "%.4f", val);
    }
}