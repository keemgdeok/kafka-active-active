package io.github.keemgdeok.kafkaactiveactive.support;

import java.util.stream.IntStream;

/** A bounded, reproducible seed selection: one seed or an inclusive range within 0..19. */
public final class FaultSeeds {
    private FaultSeeds() {}
    public static IntStream values() {
        String[] bounds = System.getProperty("kaa.fault.seeds", "0").split("\\.\\.", -1);
        int first = Integer.parseInt(bounds[0]);
        int last = bounds.length == 1 ? first : Integer.parseInt(bounds[1]);
        if (bounds.length > 2 || first < 0 || last > 19 || first > last) {
            throw new IllegalArgumentException("faultSeeds must be a seed or inclusive range within 0..19");
        }
        return IntStream.rangeClosed(first, last);
    }
}
