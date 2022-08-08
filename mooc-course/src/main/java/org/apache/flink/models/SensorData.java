package org.apache.flink.models;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class SensorData {
    @SerializedName("sensorType")
    @Expose
    private String sensorType;
    @SerializedName("value")
    @Expose
    private double value;
    @SerializedName("sensorId")
    @Expose
    private long sensorId;
    @SerializedName("timestamp")
    @Expose
    private long timestamp;

    /**
     * No args constructor for use in serialization
     *
     */
    public SensorData() {
    }

    /**
     *
     * @param sensorType
     * @param value
     * @param sensorId
     * @param timestamp
     */
    public SensorData(String sensorType, double value, long sensorId, long timestamp) {
        super();
        this.sensorType = sensorType;
        this.value = value;
        this.sensorId = sensorId;
        this.timestamp = timestamp;
    }

    public String getSensorType() {
        return sensorType;
    }

    public void setSensorType(String sensorType) {
        this.sensorType = sensorType;
    }

    public SensorData withSensorType(String sensorType) {
        this.sensorType = sensorType;
        return this;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public SensorData withValue(double value) {
        this.value = value;
        return this;
    }

    public long getSensorId() {
        return sensorId;
    }

    public void setSensorId(long sensorId) {
        this.sensorId = sensorId;
    }

    public SensorData withSensorId(long sensorId) {
        this.sensorId = sensorId;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public SensorData withTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(SensorData.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("sensorType");
        sb.append('=');
        sb.append(((this.sensorType == null)?"<null>":this.sensorType));
        sb.append(',');
        sb.append("value");
        sb.append('=');
        sb.append(this.value);
        sb.append(',');
        sb.append("sensorId");
        sb.append('=');
        sb.append(this.sensorId);
        sb.append(',');
        sb.append("timestamp");
        sb.append('=');
        sb.append(this.timestamp);
        sb.append(',');
        if (sb.charAt((sb.length()- 1)) == ',') {
            sb.setCharAt((sb.length()- 1), ']');
        } else {
            sb.append(']');
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = ((result* 31)+((int)(Double.doubleToLongBits(this.value)^(Double.doubleToLongBits(this.value)>>> 32))));
        result = ((result* 31)+((this.sensorType == null)? 0 :this.sensorType.hashCode()));
        result = ((result* 31)+((int)(this.sensorId^(this.sensorId >>> 32))));
        result = ((result* 31)+((int)(this.timestamp^(this.timestamp >>> 32))));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof SensorData)) {
            return false;
        }
        SensorData rhs = ((SensorData) other);
        return ((((Double.doubleToLongBits(this.value) == Double.doubleToLongBits(rhs.value))&&(Objects.equals(this.sensorType, rhs.sensorType)))&&(this.sensorId == rhs.sensorId))&&(this.timestamp == rhs.timestamp));
    }
}
