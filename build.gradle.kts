import java.time.Duration

plugins {
    `java-library`
    application
}

application {
    mainClass.set("io.github.keemgdeok.kafkaactiveactive.demo.DemoMain")
}

group = "io.github.keemgdeok"
version = "0.1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(21))
}

val integrationTest = sourceSets.create("integrationTest")
configurations[integrationTest.implementationConfigurationName].extendsFrom(configurations.testImplementation.get())
configurations[integrationTest.runtimeOnlyConfigurationName].extendsFrom(configurations.testRuntimeOnly.get())
val faultTest = sourceSets.create("faultTest")
configurations[faultTest.implementationConfigurationName].extendsFrom(configurations[integrationTest.implementationConfigurationName])
configurations[faultTest.runtimeOnlyConfigurationName].extendsFrom(configurations[integrationTest.runtimeOnlyConfigurationName])
val baselineTest = sourceSets.create("baselineTest")
configurations[baselineTest.implementationConfigurationName].extendsFrom(configurations[integrationTest.implementationConfigurationName])
configurations[baselineTest.runtimeOnlyConfigurationName].extendsFrom(configurations[integrationTest.runtimeOnlyConfigurationName])

dependencies {
    api("org.apache.kafka:kafka-clients:4.3.2")

    testImplementation(platform("org.junit:junit-bom:5.14.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.17")

    add(integrationTest.implementationConfigurationName, sourceSets.main.get().output)
    add(integrationTest.implementationConfigurationName, platform("org.testcontainers:testcontainers-bom:2.0.5"))
    add(integrationTest.implementationConfigurationName, "org.testcontainers:testcontainers-kafka")
    add(integrationTest.implementationConfigurationName, "org.testcontainers:testcontainers-junit-jupiter")
    add(faultTest.implementationConfigurationName, integrationTest.output)
    add(faultTest.implementationConfigurationName, "org.testcontainers:testcontainers-toxiproxy:2.0.5")
    add(baselineTest.implementationConfigurationName, integrationTest.output)
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.release.set(21)
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    maxParallelForks = 1
    // Pin Testcontainers' support images as well as the Kafka/Toxiproxy workload images.
    // These are the manifests for the framework defaults: Ryuk 0.14.0 and Alpine 3.17.
    environment("TESTCONTAINERS_RYUK_CONTAINER_IMAGE", "testcontainers/ryuk@sha256:7c1a8a9a47c780ed0f983770a662f80deb115d95cce3e2daa3d12115b8cd28f0")
    environment("TESTCONTAINERS_TINYIMAGE_CONTAINER_IMAGE", "alpine@sha256:8fc3dacfb6d69da8d44e42390de777e48577085db99aa4e4af35f483eb08b989")
    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

tasks.register<Test>("integrationTest") {
    description = "Runs tests against two real, independent Kafka containers. Requires Docker."
    group = "verification"
    testClassesDirs = integrationTest.output.classesDirs
    classpath = integrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    timeout.set(Duration.ofMinutes(15))
}

tasks.register<Test>("faultTest") {
    description = "Runs seeded network and abrupt-process fault contracts. Requires Docker."
    group = "verification"
    testClassesDirs = faultTest.output.classesDirs
    classpath = faultTest.runtimeClasspath
    systemProperty("kaa.fault.seeds", providers.gradleProperty("faultSeeds").getOrElse("0"))
    systemProperty("kaa.fault.classpath", faultTest.runtimeClasspath.asPath)
    shouldRunAfter(tasks.named("integrationTest"))
    timeout.set(Duration.ofMinutes(50))
    outputs.upToDateWhen { false }
}

tasks.register<Test>("baselineTest") {
    description = "Compares SDK and default-policy MM2 event sets and native checkpoint translation. Requires Docker."
    group = "verification"
    testClassesDirs = baselineTest.output.classesDirs
    classpath = baselineTest.runtimeClasspath
    systemProperty("kaa.baseline.count", providers.gradleProperty("baselineCount").getOrElse("24"))
    systemProperty("kaa.baseline.seed", providers.gradleProperty("baselineSeed").getOrElse("0"))
    shouldRunAfter(tasks.named("faultTest"))
    timeout.set(Duration.ofMinutes(10))
    outputs.upToDateWhen { false }
}

dependencyLocking {
    lockAllConfigurations()
}

tasks.wrapper {
    gradleVersion = "9.8.1"
    distributionType = Wrapper.DistributionType.BIN
    distributionSha256Sum = "dce76f55f8e251a3a1f130eb120f30b3d271de2b76c9b0729d316b5a1b6dc01f"
}
