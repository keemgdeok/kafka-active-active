package io.github.keemgdeok.kafkaactiveactive.verification;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;

/** Write-only diagnostic evidence. The SDK never reads this file as operational state. */
public final class JsonlEvidence implements AutoCloseable {
    private final PrintWriter output;
    public JsonlEvidence(Path file) throws IOException {
        Files.createDirectories(file.toAbsolutePath().getParent());
        output = new PrintWriter(Files.newBufferedWriter(file));
    }
    public synchronized void record(String type, Map<String, ?> fields) {
        StringBuilder line = new StringBuilder("{\"time\":").append(quote(Instant.now().toString()))
                .append(",\"type\":").append(quote(type));
        fields.forEach((key, value) -> line.append(',').append(quote(key)).append(':')
                .append(value instanceof Number || value instanceof Boolean ? value : quote(String.valueOf(value))));
        line.append('}');
        output.println(line);
        output.flush();
        if (output.checkError()) { throw new IllegalStateException("diagnostic evidence write failed"); }
        System.out.println(line);
    }
    private static String quote(String value) {
        StringBuilder escaped = new StringBuilder("\"");
        for (char character : value.toCharArray()) {
            if (character == '"' || character == '\\') { escaped.append('\\').append(character); }
            else if (character < 32) { escaped.append(String.format("\\u%04x", (int) character)); }
            else { escaped.append(character); }
        }
        return escaped.append('"').toString();
    }
    @Override public synchronized void close() { output.close(); }
}
