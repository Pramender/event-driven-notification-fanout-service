package com.eventdriven.notification.fanout.infrastructure.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "subscription_delivery_cursors")
public class SubscriptionDeliveryCursorEntity {

    @Id
    @Column(name = "subscription_id")
    private UUID subscriptionId;

    @Column(name = "next_deliverable_seq", nullable = false)
    private long nextDeliverableSeq;

    @Column(name = "next_assign_seq", nullable = false)
    private long nextAssignSeq;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public UUID getSubscriptionId() {
        return subscriptionId;
    }

    public void setSubscriptionId(UUID subscriptionId) {
        this.subscriptionId = subscriptionId;
    }

    public long getNextDeliverableSeq() {
        return nextDeliverableSeq;
    }

    public void setNextDeliverableSeq(long nextDeliverableSeq) {
        this.nextDeliverableSeq = nextDeliverableSeq;
    }

    public long getNextAssignSeq() {
        return nextAssignSeq;
    }

    public void setNextAssignSeq(long nextAssignSeq) {
        this.nextAssignSeq = nextAssignSeq;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
