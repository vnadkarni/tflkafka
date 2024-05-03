package com.vnadkarni.tfl.tflkafka;

public class VehicleEvent {
    private String vehicleId;
    private String eventType;
    private String eventLocation;
    private String timestamp;

    // Default constructor
    public VehicleEvent() {
    }

    // Constructor with all parameters
    public VehicleEvent(String vehicleId, String eventType, String eventLocation, String timestamp) {
        this.vehicleId = vehicleId;
        this.eventType = eventType;
        this.eventLocation = eventLocation;
        this.timestamp = timestamp;
    }

    // Getters
    public String getVehicleId() {
        return vehicleId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getEventLocation() {
        return eventLocation;
    }

    public String getTimestamp() {
        return timestamp;
    }

    // Setters
    public void setVehicleId(String vehicleId) {
        this.vehicleId = vehicleId;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public void setEventLocation(String eventLocation) {
        this.eventLocation = eventLocation;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}
