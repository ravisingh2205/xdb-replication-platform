package com.xdb.replication.publication.repository;

import com.xdb.replication.publication.domain.Publication;
import com.xdb.replication.publication.domain.PublicationStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface PublicationRepository extends JpaRepository<Publication, Long> {

    Optional<Publication> findByName(String name);

    boolean existsByName(String name);

    boolean existsBySourceDatabaseId(Long sourceDatabaseId);

    List<Publication> findByStatusIn(Collection<PublicationStatus> statuses);
}
