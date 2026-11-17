import java.time.Duration

plugins {
    `java-library`
    `maven-publish`
    application
    signing
}

application {
    mainClass.set("io.github.keemgdeok.kafkaactiveactive.demo.DemoMain")
}

tasks.register<JavaExec>("runProducerExample") {
    group = "application"
    description = "Runs the producer example with an opt-in replication role."
    mainClass.set("io.github.keemgdeok.kafkaactiveactive.demo.ProducerExample")
    classpath = sourceSets.main.get().runtimeClasspath
    standardInput = System.`in`
}

group = "io.github.keemgdeok"
val releaseVersion = providers.gradleProperty("releaseVersion")
version = releaseVersion.getOrElse("0.1.0-SNAPSHOT")

repositories {
    mavenCentral()
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(21))
    withSourcesJar()
    withJavadocJar()
}

tasks.jar {
    from("LICENSE") { into("META-INF") }
}

tasks.javadoc {
    (options as StandardJavadocDocletOptions).addBooleanOption("Xdoclint:all,-missing", true)
}

val stagingRepository = layout.buildDirectory.dir("maven-staging")
val repositoryPath = "${group.toString().replace('.', '/')}/${project.name}/${version}"
val projectUrl = "https://github.com/keemgdeok/kafka-active-active"

publishing {
    publications {
        create<MavenPublication>("sdk") {
            from(components["java"])
            pom {
                name.set("Kafka Active-Active")
                description.set("Java SDK for key-routed publishing and same-partition replication across two independent Kafka deployments.")
                url.set(projectUrl)
                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }
                developers {
                    developer {
                        id.set("keemgdeok")
                        name.set("keemgdeok")
                        url.set("https://github.com/keemgdeok")
                    }
                }
                scm {
                    url.set(projectUrl)
                    connection.set("scm:git:$projectUrl.git")
                    developerConnection.set("scm:git:ssh://git@github.com/keemgdeok/kafka-active-active.git")
                }
            }
        }
    }
    repositories {
        maven {
            name = "staging"
            url = uri(stagingRepository)
        }
    }
}

val signingKey = providers.environmentVariable("KAA_SIGNING_KEY")
val signingPassword = providers.environmentVariable("KAA_SIGNING_PASSWORD")
signing {
    isRequired = signingKey.isPresent
    useInMemoryPgpKeys(signingKey.orNull, signingPassword.orNull)
    sign(publishing.publications["sdk"])
}

tasks.register<Zip>("centralBundle") {
    group = "publishing"
    description = "Bundles the signed release for upload to Sonatype Central."
    dependsOn("publishSdkPublicationToStagingRepository")
    archiveClassifier.set("central")
    destinationDirectory.set(layout.buildDirectory.dir("releases"))
    from(stagingRepository.map { it.dir(repositoryPath) }) {
        into(repositoryPath)
    }
    doFirst {
        check(releaseVersion.orNull?.matches(Regex("""\d+\.\d+\.\d+""")) == true) {
            "Set -PreleaseVersion to a release version such as 0.1.0."
        }
        check(signingKey.isPresent) { "Set KAA_SIGNING_KEY to an ASCII-armored OpenPGP secret key." }
    }
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
    timeout.set(Duration.ofMinutes(58))
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
