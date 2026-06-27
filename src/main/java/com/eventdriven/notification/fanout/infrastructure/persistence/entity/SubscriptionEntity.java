package com.eventdriven.notification.fanout.infrastructure.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "subscriptions")
public class SubscriptionEntity {

    @Id
    @Column(name = "subscription_id")
    private UUID subscriptionId;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private boolean enabled;

    @Column(name = "delivery_mode", nullable = false)
    private String deliveryMode;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "filter_json", nullable = false, columnDefinition = "jsonb")
    private String filterJson;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "target_json", nullable = false, columnDefinition = "jsonb")
    private String targetJson;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "retry_policy_json", nullable = false, columnDefinition = "jsonb")
    private String retryPolicyJson;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(nullable = false)
    private boolean deleted;

    public UUID getSubscriptionId() {
        return subscriptionId;
    }

    public void setSubscriptionId(UUID subscriptionId) {
        this.subscriptionId = subscriptionId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getDeliveryMode() {
        return deliveryMode;
    }

    public void setDeliveryMode(String deliveryMode) {
        this.deliveryMode = deliveryMode;
    }

    public String getFilterJson() {
        return filterJson;
    }

    public void setFilterJson(String filterJson) {
        this.filterJson = filterJson;
    }

    public String getTargetJson() {
        return targetJson;
    }

    public void setTargetJson(String targetJson) {
        this.targetJson = targetJson;
    }

    public String getRetryPolicyJson() {
        return retryPolicyJson;
    }

    public void setRetryPolicyJson(String retryPolicyJson) {
        this.retryPolicyJson = retryPolicyJson;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }
}
