package ds200.lab03.model;
import java.io.Serializable;

public record User(int id, String sex, int ageVal, int jobId) implements Serializable {}