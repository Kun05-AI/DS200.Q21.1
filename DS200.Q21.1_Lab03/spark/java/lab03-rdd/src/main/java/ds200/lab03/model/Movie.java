package ds200.lab03.model;
import java.io.Serializable;
import java.util.List;

public record Movie(int id, String name, List<String> categories) implements Serializable {}