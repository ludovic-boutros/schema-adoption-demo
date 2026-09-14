package com.example.kafka.producer;

/**
 * The producer's own view of an order event. Deliberately a separate class from
 * {@link com.example.kafka.consumer.OrderEvent} — in a real system these would be two independent
 * services, each maintaining its own copy of the data model, which is exactly why they can drift
 * apart without anyone noticing.
 *
 * Attempted breaking change: a developer switches {@code amount} from a {@code String} to a proper
 * {@code Double} again - the exact same mistake as branch 02. This time Schema Registry's
 * compatibility check catches it before this code can send a single message.
 */
public class OrderEvent {

    private String orderId;
    private String customerId;
    private Double amount;
    private String status;

    public OrderEvent() {
    }

    public OrderEvent(String orderId, String customerId, Double amount, String status) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.status = status;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "OrderEvent{orderId='" + orderId + "', customerId='" + customerId
                + "', amount=" + amount + " (Double), status='" + status + "'}";
    }
}
