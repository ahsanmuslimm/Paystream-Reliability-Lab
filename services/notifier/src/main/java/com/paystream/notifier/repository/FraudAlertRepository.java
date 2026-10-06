package com.paystream.notifier.repository;

import com.paystream.notifier.domain.FraudAlertEntity;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface FraudAlertRepository extends JpaRepository<FraudAlertEntity, UUID> {
}
