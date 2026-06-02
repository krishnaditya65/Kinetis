plugins {
    // Lets Gradle auto-download a JDK 21 toolchain when one isn't installed locally.
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.9.0"
}

rootProject.name = "kinetis"

include("raft")
include("scheduler-core")
include("worker")
include("api")
