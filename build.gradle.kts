/*
 * User Manual available at https://docs.gradle.org/7.4/userguide/building_java_projects.html
 */


group = "at.willhaben.kafka.connect.transforms.jslt"
version = System.getenv("VERSION") ?: "1.0.0"

val javaVersion = 11


plugins {
    kotlin("jvm") version "1.6.10"
    idea // Generates files that are used by IntelliJ IDEA, thus making it possible to open the project from IDEA
    `java-library` // Apply the java-library plugin for API and implementation separation.
    `maven-publish`
}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    val kafkaConnectVersion = "3.1.0"
    val jsltLibVersion = "0.1.11"
    val junitVersion = "5.8.2"

    implementation(platform("org.jetbrains.kotlin:kotlin-bom")) // Align versions of all Kotlin components
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8") // Use the Kotlin JDK 8 standard library.

    implementation("org.apache.kafka:connect-api:$kafkaConnectVersion")
    implementation("org.apache.kafka:connect-json:$kafkaConnectVersion")
    implementation("org.apache.kafka:connect-transforms:$kafkaConnectVersion")
    implementation("com.schibsted.spt.data:jslt:$jsltLibVersion")


    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
}

kotlin {
    jvmToolchain {
        (this as JavaToolchainSpec).languageVersion.set(JavaLanguageVersion.of(javaVersion))
    }
}


tasks.jar {
    manifest {
        attributes(
            mapOf(
                "Implementation-Title" to project.name,
                "Implementation-Version" to project.version
            )
        )
    }
}


publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = project.group.toString()
            artifactId = project.name
            version = project.version.toString()

            from(components["java"])
        }
    }
    repositories {
        maven {
            name = "ArtifactoryLocal"
            url = uri(project.properties.getOrDefault("artifactory_context", System.getenv("ARTIFACTORY_CONTEXT")).toString() + "/libs-release-local")
            credentials {
                username = project.properties.getOrDefault("artifactory_user", System.getenv("ARTIFACTORY_USER")).toString()
                password = project.properties.getOrDefault("artifactory_password", System.getenv("ARTIFACTORY_PWD")).toString()
            }
        }
    }
}

