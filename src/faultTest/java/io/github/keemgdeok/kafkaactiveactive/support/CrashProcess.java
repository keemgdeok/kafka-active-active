package io.github.keemgdeok.kafkaactiveactive.support;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/** A separate JVM makes halt tests exercise actual lost memory, sockets and orphan transactions. */
public final class CrashProcess {
    public static final int EXPECTED_EXIT = 73;
    private CrashProcess() {}
    public static void run(String main, String label, String... arguments) throws Exception {
        Path log = Path.of("build", "reports", "faultTest", label + ".log").toAbsolutePath();
        Files.createDirectories(log.getParent());
        var command = new ArrayList<>(Arrays.asList(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx192m", "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn", "-cp",
                System.getProperty("kaa.fault.classpath"), main));
        command.addAll(Arrays.asList(arguments));
        Process child = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            if (!child.waitFor(45, TimeUnit.SECONDS)) { throw new AssertionError("child did not reach fault: " + log); }
            String output = Files.readString(log);
            if (child.exitValue() != EXPECTED_EXIT || !output.contains("FAULT_POINT")) {
                throw new AssertionError("unexpected child exit " + child.exitValue() + ": " + log + "\n" + output);
            }
        } finally {
            if (child.isAlive()) { child.destroyForcibly(); }
            if (!child.waitFor(5, TimeUnit.SECONDS)) { throw new AssertionError("child termination not confirmed: " + log); }
        }
    }
    public static void halt(String point) {
        System.out.println("FAULT_POINT " + point);
        System.out.flush();
        Runtime.getRuntime().halt(EXPECTED_EXIT);
    }
}
