package com.xdb.replication.subscription.repository;

import com.xdb.replication.subscription.domain.Subscription;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface SubscriptionRepository extends JpaRepository<Subscription, Long> {

    Optional<Subscription> findByName(String name);

    boolean existsByName(String name);

    boolean existsByTargetDatabaseId(Long targetDatabaseId);

    List<Subscription> findByPublicationName(String publicationName);
}
