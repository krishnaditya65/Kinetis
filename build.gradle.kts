plugins {
    java
    id("org.springframework.boot") version "3.3.5" apply false
    id("io.spring.dependency-management") version "1.1.6" apply false
    // Protobuf plugin declared here so the version is managed centrally; applied in api/ only.
    id("com.google.protobuf") version "0.9.4" apply false
}

allprojects {
    group = "io.kinetis"
    version = "0.1.0"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java")
    apply(plugin = "io.spring.dependency-management")

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
    }

    // Import the Spring Boot BOM in every module so versions are managed centrally.
    the<io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension>().apply {
        imports {
            mavenBom("org.springframework.boot:spring-boot-dependencies:3.3.5")
        }
    }

    dependencies {
        "testImplementation"("org.junit.jupiter:junit-jupiter")
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
        }
        // Testcontainers + Docker Desktop on macOS: point at the per-user socket and force an
        // API version the daemon accepts (its MinAPIVersion is newer than docker-java's default).
        val dockerHost = System.getenv("DOCKER_HOST")
            ?: "unix://${System.getProperty("user.home")}/.docker/run/docker.sock"
        environment("DOCKER_HOST", dockerHost)
        environment("DOCKER_API_VERSION", System.getenv("DOCKER_API_VERSION") ?: "1.44")
        environment("TESTCONTAINERS_RYUK_DISABLED", "true")
        // docker-java reads its negotiated API version from the `api.version` system property.
        // The local Docker Engine's MinAPIVersion (1.44) is newer than docker-java's built-in
        // default, so we pin it explicitly to avoid a 400 on /info.
        systemProperty("api.version", System.getProperty("api.version") ?: "1.44")
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.compilerArgs.add("-parameters")
    }
}
