plugins {
    alias(libs.plugins.kotlin.jvm)
}

group = "sk.bakeit.lease"
version = "unspecified"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":lease-api"))
    implementation(libs.jdbc.driver.postgresql)

    testImplementation(libs.google.truth)
    testImplementation(libs.hikari.cp)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.mockito.junit.jupiter)
    testImplementation(libs.mockito.kotlin)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}