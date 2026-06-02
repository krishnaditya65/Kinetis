// scheduler-core: the durable primitive. Framework-light — plain Java + Spring JDBC.
dependencies {
    implementation(project(":raft"))
    implementation("org.springframework:spring-jdbc")
    implementation("org.springframework:spring-context")
    implementation("org.postgresql:postgresql")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("org.flywaydb:flyway-core")
    implementation("org.flywaydb:flyway-database-postgresql")
    implementation("io.micrometer:micrometer-core")
    implementation("org.slf4j:slf4j-api")
    // etcd Java client for EtcdShardOwnership (Phase 3c)
    implementation("io.etcd:jetcd-core:0.7.7")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.testcontainers:postgresql:1.21.3")
    testImplementation("org.testcontainers:junit-jupiter:1.21.3")
}
