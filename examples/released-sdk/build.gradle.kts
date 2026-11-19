plugins {
    application
}

repositories {
    mavenCentral()
}

val sdkVersion = providers.gradleProperty("sdkVersion")

dependencies {
    implementation("io.github.keemgdeok:kafka-active-active:${sdkVersion.get()}")
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(21))
}

application {
    mainClass.set("ReleasedSdkCheck")
}
