package com.flinklearn.realtime.chapter2;

public class AuditTrail {


    int id;
    String user;
    String entity;
    String operation;
    long timestamp;
    int duration;
    int count;

    //Convert String array to AuditTrail object
    public AuditTrail(String auditStr) {

        //Split the string
        String[] attributes = auditStr
                                .replace("\"","")
                                .split(",");

        //Assign values
        this.id = Integer.parseInt(attributes[0]);
        this.user = attributes[1];
        this.entity = attributes[2];
        this.operation = attributes[3];
        this.timestamp = Long.parseLong(attributes[4]);
        this.duration = Integer.parseInt(attributes[5]);
        this.count = Integer.parseInt(attributes[6]);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "AuditTrail{" +
                "id=" + id +
                ", user='" + user + '\'' +
                ", entity='" + entity + '\'' +
                ", operation='" + operation + '\'' +
                ", timestamp=" + timestamp +
                ", duration=" + duration +
                ", count=" + count +
                '}';
    }

}
