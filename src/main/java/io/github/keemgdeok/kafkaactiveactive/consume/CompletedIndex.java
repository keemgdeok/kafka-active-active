package io.github.keemgdeok.kafkaactiveactive.consume;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

final class CompletedIndex {
    private final ConsumerSettings settings;
    private final Map<String, CompletionMarker> entries = new HashMap<>();
    private long bytes;
    CompletedIndex(ConsumerSettings settings) { this.settings = settings; }
    boolean contains(CompletionMarker marker) {
        CompletionMarker old = entries.get(marker.id());
        if (old == null) { return false; }
        if (!old.matches(marker)) { throw new IllegalStateException("completion ID/key/payload conflict"); }
        return true;
    }
    void add(CompletionMarker marker) {
        if (!contains(marker)) {
            long cost = 192L + marker.id().length() * 2L;
            if (entries.size() >= settings.maxCompletedEntries() || cost > settings.maxCompletedBytes() - bytes) {
                throw new IllegalStateException("completion index limit exceeded");
            }
            entries.put(marker.id(), marker);
            bytes += cost;
        }
    }
    void reserve(Collection<CompletionMarker> markers) {
        Map<String, CompletionMarker> extra = new HashMap<>();
        long extraBytes = 0;
        for (var marker : markers) {
            if (!contains(marker)) {
                var previous = extra.putIfAbsent(marker.id(), marker);
                if (previous != null && !previous.matches(marker)) {
                    throw new IllegalStateException("batch completion conflict");
                }
                if (previous == null) { extraBytes += 192L + marker.id().length() * 2L; }
            }
        }
        if (extra.size() > settings.maxCompletedEntries() - entries.size()
                || extraBytes > settings.maxCompletedBytes() - bytes) {
            throw new IllegalStateException("completion index limit exceeded");
        }
    }
    int size() { return entries.size(); }
    Collection<CompletionMarker> markers() { return java.util.List.copyOf(entries.values()); }
}
