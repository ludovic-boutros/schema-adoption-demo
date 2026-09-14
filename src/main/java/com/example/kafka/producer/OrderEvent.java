package com.example.kafka.producer;

/**
 * The producer's own view of an order event. Deliberately a separate class from
 * {@link com.example.kafka.consumer.OrderEvent} — in a real system these would be two independent
 * services, each maintaining its own copy of the data model, which is exactly why they can drift
 * apart without anyone noticing.
 *
 * The branch 04 retype attempt is abandoned - {@code amount} goes back to being a {@code String}.
 * Instead, a new, optional numeric field is added alongside it: {@code amountNumeric}, for
 * services (e.g. billing analytics) that want to do math on the total without parsing a string
 * themselves. Nothing existing was removed or retyped, so this is a safe evolution.
 */
public class OrderEvent {

    private String orderId;
    private String customerId;
    private String amount;
    private Double amountNumeric;
    private String status;

    public OrderEvent() {
    }

    public OrderEvent(String orderId, String customerId, String amount, Double amountNumeric, String status) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.amountNumeric = amountNumeric;
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

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public Double getAmountNumeric() {
        return amountNumeric;
    }

    public void setAmountNumeric(Double amountNumeric) {
        this.amountNumeric = amountNumeric;
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
                + "', amount='" + amount + "' (String), amountNumeric=" + amountNumeric
                + ", status='" + status + "'}";
    }
}
