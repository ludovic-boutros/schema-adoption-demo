package com.example.kafka.consumer;

/**
 * The consumer's own view of an order event. Deliberately a separate class from
 * {@link com.example.kafka.producer.OrderEvent} — in a real system these would be two independent
 * services, each maintaining its own copy of the data model, which is exactly why they can drift
 * apart without anyone noticing.
 */
public class OrderEvent {

    private String orderId;
    private String customerId;
    private String amount;
    private String status;

    public OrderEvent() {
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

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
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
                + "', amount='" + amount + "' (String), status='" + status + "'}";
    }
}
