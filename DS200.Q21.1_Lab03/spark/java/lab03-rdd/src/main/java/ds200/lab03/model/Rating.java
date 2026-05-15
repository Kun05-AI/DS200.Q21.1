package ds200.lab03.model;
import java.io.Serializable;

public record Rating(int uid, int mid, double score, long time) implements Serializable {}