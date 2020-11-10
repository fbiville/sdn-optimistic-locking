package org.neo4j.sdnlegacy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.ogm.exception.OptimisticLockingException;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;
import org.neo4j.sdnlegacy.movie.MovieRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.neo4j.DataNeo4jTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@DataNeo4jTest
class SdnOgmOptimisticLockTest {

    @Autowired
    private MovieRepository movieRepository;

    @Autowired
    private SessionFactory sessionFactory;

    @Container
    private static final Neo4jContainer<?> neo4jContainer = new Neo4jContainer<>("neo4j:4.0");

    @DynamicPropertySource
    static void neo4jProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.neo4j.uri", neo4jContainer::getBoltUrl);
        registry.add("spring.data.neo4j.username", () -> "neo4j");
        registry.add("spring.data.neo4j.password", neo4jContainer::getAdminPassword);
    }

    @BeforeEach
    void setup() throws IOException {
        try (BufferedReader moviesReader = new BufferedReader(
                new InputStreamReader(this.getClass().getResourceAsStream("/movies.cypher")))) {
            Session session = sessionFactory.openSession();
            session.query("MATCH (n) DETACH DELETE n", emptyMap());
            String moviesCypher = moviesReader.lines().collect(Collectors.joining(" "));
            session.query(moviesCypher, emptyMap());
        }
    }

    @Test
    void movie_deletion_fails_due_to_optimistic_locking_issue() {
        assertThatThrownBy(() -> movieRepository.deleteById("The Matrix"))
                .isInstanceOf(OptimisticLockingException.class);
    }
}