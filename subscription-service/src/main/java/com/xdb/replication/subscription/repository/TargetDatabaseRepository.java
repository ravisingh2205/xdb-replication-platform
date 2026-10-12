package com.xdb.replication.subscription.repository;

import com.xdb.replication.subscription.domain.TargetDatabase;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface TargetDatabaseRepository extends JpaRepository<TargetDatabase, Long> {

    Optional<TargetDatabase> findByName(String name);

    boolean existsByName(String name);
}
