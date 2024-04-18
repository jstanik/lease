plugins {
    alias(libs.plugins.kotlin.jvm)
}

group = "sk.bakeit.lease"
version = "unspecified"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":api"))
    implementation(libs.jdbc.driver.postgresql)

    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.hikari.cp)
    testImplementation(libs.google.truth)
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}