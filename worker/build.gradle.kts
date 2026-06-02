// worker: execution side. Depends on scheduler-core for the store/lease protocol.
dependencies {
    implementation(project(":scheduler-core"))
    implementation("org.springframework:spring-context")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("io.micrometer:micrometer-core")
    implementation("org.slf4j:slf4j-api")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}
