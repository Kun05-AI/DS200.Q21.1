package ds200.lab03.model;
import java.io.Serializable;

public record RatingStats(double totalScore, long recordCount) implements Serializable {
    public double calcAvg() {
        return recordCount == 0 ? 0.0 : totalScore / recordCount;
    }
    public static RatingStats combine(RatingStats a, RatingStats b) {
        return new RatingStats(a.totalScore() + b.totalScore(), a.recordCount() + b.recordCount());
    }
}