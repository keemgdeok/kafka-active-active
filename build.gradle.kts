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
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.release.set(21)
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    maxParallelForks = 1
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

dependencyLocking {
    lockAllConfigurations()
}

tasks.wrapper {
    gradleVersion = "9.8.1"
    distributionType = Wrapper.DistributionType.BIN
    distributionSha256Sum = "dce76f55f8e251a3a1f130eb120f30b3d271de2b76c9b0729d316b5a1b6dc01f"
}
