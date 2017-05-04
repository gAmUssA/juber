package com.juber.model;

public class Message {
    private String driver;
    private LngLat lngLat;
    private String rider;
    private String status;
    private long timestamp;

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public LngLat getLngLat() {
        return lngLat;
    }

    public void setLngLat(LngLat lngLat) {
        this.lngLat = lngLat;
    }

    public String getRider() {
        return rider;
    }

    public void setRider(String rider) {
        this.rider = rider;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Message)) {
            return false;
        }

        Message message = (Message) o;

        if (timestamp != message.timestamp) {
            return false;
        }
        if (driver != null ? !driver.equals(message.driver) : message.driver != null) {
            return false;
        }
        if (lngLat != null ? !lngLat.equals(message.lngLat) : message.lngLat != null) {
            return false;
        }
        if (rider != null ? !rider.equals(message.rider) : message.rider != null) {
            return false;
        }
        return status != null ? status.equals(message.status) : message.status == null;
    }

    @Override
    public int hashCode() {
        int result = driver != null ? driver.hashCode() : 0;
        result = 31 * result + (lngLat != null ? lngLat.hashCode() : 0);
        result = 31 * result + (rider != null ? rider.hashCode() : 0);
        result = 31 * result + (status != null ? status.hashCode() : 0);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Message{" + "driver='" + driver + '\'' + ", lngLat=" + lngLat + ", rider='" + rider + '\'' + ", status='" + status
                + '\'' + ", timestamp=" + timestamp + '}';
    }
}
