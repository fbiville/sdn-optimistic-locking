package org.neo4j.sdnlegacy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.sdnlegacy.movie.MovieRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.neo4j.DataNeo4jTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.neo4j.core.ReactiveDatabaseSelectionProvider;
import org.springframework.data.neo4j.core.transaction.ReactiveNeo4jTransactionManager;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

@Testcontainers
@DataNeo4jTest
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class Sdn6OptimisticLockTest {

    @Autowired
    private Driver driver;

    @Autowired
    private MovieRepository movieRepository;

    @Container
    private static final Neo4jContainer<?> neo4jContainer = new Neo4jContainer<>("neo4j:4.0");

    @DynamicPropertySource
    static void neo4jProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.neo4j.uri", neo4jContainer::getBoltUrl);
        registry.add("spring.neo4j.authentication.username", () -> "neo4j");
        registry.add("spring.neo4j.authentication.password", neo4jContainer::getAdminPassword);
    }

    @BeforeEach
    void setup() throws IOException {
        try (BufferedReader moviesReader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/movies.cypher")));
             Session session = driver.session()) {

            session.writeTransaction(tx -> {
                String moviesCypher = moviesReader.lines().collect(Collectors.joining(" "));

                tx.run("MATCH (n) DETACH DELETE n");
                tx.run(moviesCypher);
                return null;
            });
        }
    }

    @Test
    void movie_deletion_fails_due_to_optimistic_locking_issue() {
        StepVerifier.create(movieRepository.deleteById("The Matrix"))
                .expectError(OptimisticLockingFailureException.class)
                .verify();
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class Configuration {

        @Bean
        public ReactiveNeo4jTransactionManager reactiveTransactionManager(Driver driver,
                                                                          ReactiveDatabaseSelectionProvider databaseSelectionProvider) {

            return new ReactiveNeo4jTransactionManager(driver, databaseSelectionProvider);
        }

    }
}
