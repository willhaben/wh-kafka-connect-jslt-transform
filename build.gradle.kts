/*
 * User Manual available at https://docs.gradle.org/7.4/userguide/building_java_projects.html
 */


group = "at.willhaben.kafka.connect.transforms.jslt"
version = System.getenv("VERSION") ?: "1.0.0"

val javaVersion = 11

val artifactoryContext =
    project.properties.getOrDefault("artifactory_context", System.getenv("ARTIFACTORY_CONTEXT")).toString()
val artifactoryUsern =
    project.properties.getOrDefault("artifactory_user", System.getenv("ARTIFACTORY_USER")).toString()
val artifactoryPassword =
    project.properties.getOrDefault("artifactory_password", System.getenv("ARTIFACTORY_PWD")).toString()


plugins {
    kotlin("jvm") version "1.6.21"
    idea // Generates files that are used by IntelliJ IDEA, thus making it possible to open the project from IDEA
    `java-library` // Apply the java-library plugin for API and implementation separation.
    `maven-publish`
    id("io.gitlab.arturbosch.detekt") version "1.20.0"
}

repositories {
    mavenCentral()
}

val includeInJar by configurations.creating {
    isTransitive = false
}

dependencies {
    val kafkaConnectVersion = "3.2.+"
    val jsltLibVersion = "0.1.12"
    val junitVersion = "5.8.2"

    compileOnly(platform("org.jetbrains.kotlin:kotlin-bom")) // Align versions of all Kotlin components
    compileOnly("org.jetbrains.kotlin:kotlin-stdlib-jdk8") // Use the Kotlin JDK 8 standard library.

    implementation("org.apache.kafka:connect-api:$kafkaConnectVersion")
    implementation("org.apache.kafka:connect-json:$kafkaConnectVersion")
    implementation("org.apache.kafka:connect-transforms:$kafkaConnectVersion")
    implementation("com.schibsted.spt.data:jslt:$jsltLibVersion")
    includeInJar("com.schibsted.spt.data:jslt:$jsltLibVersion") // explicitly include this file in the build step

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
    from(zipTree(includeInJar.singleFile))
}


publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = project.group.toString()
            artifactId = project.name
            version = project.version.toString()

            pom {
                name.set( "wh-kafka-connect-jslt-transform")
                description.set( project.description)
                url.set("https://github.com/willhaben/wh-kafka-connect-jslt-transform")
                organization {
                    name.set("willhaben")
                    url.set("https://github.com/willhaben/")
                }
                issueManagement {
                    system.set("GitHub")
                    url.set("https://github.com/willhaben/wh-kafka-connect-jslt-transform/issues")
                }
                licenses {
                    license {
                        name.set( "Apache License 2.0")
                        url.set("https://github.com/willhaben/wh-kafka-connect-jslt-transform/blob/master/LICENSE")
                        distribution.set("repo")
                    }
                }
                developers {
                    developer {
                        name.set("willhaben")
                    }
                }
                scm {
                    url.set("https://github.com/willhaben/wh-kafka-connect-jslt-transform")
                    connection.set("scm:git:git://github.com/willhaben/wh-kafka-connect-jslt-transform.git")
                    developerConnection.set("scm:git:ssh://git@github.com:willhaben/wh-kafka-connect-jslt-transform.git")
                }
            }

            from(components["java"])
        }
    }
    repositories {
        maven {
            name = "ArtifactoryLocal"
            url = uri(artifactoryContext + "/libs-release-local")
            credentials {
                username = artifactoryUsern
                password = artifactoryPassword
            }
        }
    }
}

