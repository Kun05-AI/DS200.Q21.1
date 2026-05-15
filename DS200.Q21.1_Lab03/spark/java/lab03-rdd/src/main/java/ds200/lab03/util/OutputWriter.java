package ds200.lab03.util;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.net.URI;
import java.util.List;
import java.util.Objects;

public final class OutputWriter {
    private OutputWriter() {}

    /**
     * Accepts either a plain Windows path (E:/...) or a file URI (file:///E:/...).
     */
    public static void writeLines(String filePath, List<String> data) throws Exception {
        Objects.requireNonNull(filePath, "filePath");
        Path path;
        if (filePath.startsWith("file:/") || filePath.startsWith("file:\\")) {
            // treat as URI
            URI uri = URI.create(filePath);
            path = Paths.get(uri);
        } else {
            // treat as normal filesystem path
            path = Paths.get(filePath);
        }

        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.write(path, data, StandardCharsets.UTF_8);
        System.out.println("Wrote " + data.size() + " lines to " + path.toAbsolutePath());
    }
}
