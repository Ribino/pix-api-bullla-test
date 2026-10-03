package com.bullla.pix.api.application.usecase.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.bullla.pix.api.PostgresIntegrationTestBase;
import com.bullla.pix.api.domain.model.outbox.OutboxEvent;
import com.bullla.pix.api.domain.repository.outbox.OutboxEventRepository;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class OutboxClaimConcurrencyIT extends PostgresIntegrationTestBase {

    private static final Duration STUCK_AFTER = Duration.ofMinutes(5);

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private DataSource dataSource;

    @Test
    void concurrentClaimsNeverSelectTheSameRow() throws Exception {
        for (int i = 1; i <= 10; i++) {
            insertPending("tx-" + i);
        }

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch start = new CountDownLatch(1);
            Future<List<OutboxEvent>> first = pool.submit(() -> {
                start.await();
                return outboxEventRepository.claimPendingBatch(10, STUCK_AFTER);
            });
            Future<List<OutboxEvent>> second = pool.submit(() -> {
                start.await();
                return outboxEventRepository.claimPendingBatch(10, STUCK_AFTER);
            });
            start.countDown();

            Set<UUID> firstIds = idsOf(first.get(30, TimeUnit.SECONDS));
            Set<UUID> secondIds = idsOf(second.get(30, TimeUnit.SECONDS));

            Set<UUID> intersection = new HashSet<>(firstIds);
            intersection.retainAll(secondIds);
            assertThat(intersection).isEmpty();
            Set<UUID> union = new HashSet<>(firstIds);
            union.addAll(secondIds);
            assertThat(union).hasSize(10);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void skipLockedSkipsRowsHeldByAnotherTransaction() throws Exception {
        for (int i = 1; i <= 10; i++) {
            insertPending("tx-" + i);
        }

        try (Connection holder = dataSource.getConnection()) {
            holder.setAutoCommit(false);
            try (Statement statement = holder.createStatement();
                    ResultSet locked = statement.executeQuery(
                            "SELECT id FROM outbox_event WHERE status = 'PENDING' ORDER BY created_at FOR UPDATE LIMIT 5")) {
                Set<UUID> lockedIds = new HashSet<>();
                while (locked.next()) {
                    lockedIds.add(locked.getObject("id", UUID.class));
                }
                assertThat(lockedIds).hasSize(5);

                List<OutboxEvent> claimed = outboxEventRepository.claimPendingBatch(10, STUCK_AFTER);

                assertThat(idsOf(claimed)).hasSize(5);
                assertThat(idsOf(claimed)).doesNotContainAnyElementsOf(lockedIds);
            } finally {
                holder.rollback();
            }
        }
    }

    @Test
    void stuckPublishingRowsAreRecoveredWhileFreshOnesAreNot() {
        UUID stuckId = insertPending("tx-stuck");
        UUID freshId = insertPending("tx-fresh");

        List<OutboxEvent> claimed = outboxEventRepository.claimPendingBatch(10, STUCK_AFTER);
        assertThat(idsOf(claimed)).containsExactlyInAnyOrder(stuckId, freshId);
        assertThat(outboxEventRepository.claimPendingBatch(10, STUCK_AFTER)).isEmpty();

        jdbcTemplate.update(
                "UPDATE outbox_event SET claimed_at = ? WHERE id = ?",
                OffsetDateTime.now(ZoneOffset.UTC).minusMinutes(10), stuckId);

        List<OutboxEvent> recovered = outboxEventRepository.claimPendingBatch(10, STUCK_AFTER);
        assertThat(idsOf(recovered)).containsExactly(stuckId);
        assertThat(attemptsOf(stuckId)).isEqualTo(2);
        assertThat(attemptsOf(freshId)).isEqualTo(1);
    }

    private UUID insertPending(String transactionId) {
        UUID id = UUID.randomUUID();
        jdbcTemplate.update(
                "INSERT INTO outbox_event (id, aggregate_id, event_type, payload, status, attempts, created_at, published_at)"
                        + " VALUES (?, ?, 'PIX_REQUESTED', CAST(? AS jsonb), 'PENDING', 0, ?, NULL)",
                id, transactionId, "{\"transactionId\":\"" + transactionId + "\"}",
                OffsetDateTime.now(ZoneOffset.UTC));
        return id;
    }

    private Set<UUID> idsOf(List<OutboxEvent> events) {
        List<UUID> ids = new ArrayList<>();
        events.forEach(event -> ids.add(event.id()));
        return new HashSet<>(ids);
    }

    private int attemptsOf(UUID id) {
        Integer attempts = jdbcTemplate.queryForObject(
                "SELECT attempts FROM outbox_event WHERE id = ?", Integer.class, id);
        return attempts == null ? 0 : attempts;
    }
}
