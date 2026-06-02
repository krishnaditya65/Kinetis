package io.kinetis.core;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;

/**
 * Base for tests that need a real Postgres. No mocks — leasing/fencing semantics only exist
 * in the database. A single container is reused across the class; tables are truncated before each test.
 */
@Testcontainers
public abstract class AbstractPgTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kinetis")
            .withUsername("kinetis")
            .withPassword("kinetis");

    protected DataSource dataSource;
    protected JdbcTemplate jdbc;

    @BeforeEach
    void setUpDatabase() {
        DriverManagerDataSource ds = new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        ds.setDriverClassName("org.postgresql.Driver");
        this.dataSource = ds;
        this.jdbc = new JdbcTemplate(ds);

        Flyway.configure()
                .dataSource(ds)
                .locations("classpath:db/migration")
                .cleanDisabled(false)
                .load()
                .migrate();

        jdbc.execute("TRUNCATE job_runs, jobs, outbox RESTART IDENTITY CASCADE");
    }
}
