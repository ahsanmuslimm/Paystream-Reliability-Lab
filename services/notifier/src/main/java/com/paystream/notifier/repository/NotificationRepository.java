package com.paystream.notifier.repository;

import com.paystream.notifier.domain.NotificationEntity;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface NotificationRepository extends JpaRepository<NotificationEntity, UUID> {

    long countByAlertId(UUID alertId);
}
