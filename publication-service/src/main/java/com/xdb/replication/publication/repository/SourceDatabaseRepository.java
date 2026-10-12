package com.xdb.replication.publication.repository;

import com.xdb.replication.publication.domain.SourceDatabase;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface SourceDatabaseRepository extends JpaRepository<SourceDatabase, Long> {

    Optional<SourceDatabase> findByName(String name);

    boolean existsByName(String name);
}
