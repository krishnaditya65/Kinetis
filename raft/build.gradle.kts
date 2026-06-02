// raft: standalone Raft consensus implementation. Zero Spring/framework deps — pure Java.
// Other modules depend on this for the RaftShardOwnership implementation.
dependencies {
    implementation("org.slf4j:slf4j-api")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
}
