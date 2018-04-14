package com.praveen.kafka.frauddetection;

import java.util.ArrayList;
import java.util.List;

public class Aggregated {
    public void setAvgAmount(float avgAmount) {
        this.avgAmount = avgAmount;
    }

    public float getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(float totalAmount) {
        this.totalAmount = totalAmount;
    }

    public int getNoOfTransacions() {
        return noOfTransacions;
    }

    public void setNoOfTransacions(int noOfTransacions) {
        this.noOfTransacions = noOfTransacions;
    }

    public float getMaxAmount() {
        return maxAmount;
    }

    public void setMaxAmount(float maxAmount) {
        this.maxAmount = maxAmount;
    }

    public List<EnrichedTransaction> getTransactions() {
        return transactions;
    }

    public void setTransactions(List<EnrichedTransaction> transactions) {
        this.transactions = transactions;
    }

    private float avgAmount;
    private float totalAmount;
    private int noOfTransacions;
    private float maxAmount;
    List<EnrichedTransaction> transactions = new ArrayList<>();

    public Aggregated add(EnrichedTransaction transaction) {
        noOfTransacions += 1;
        totalAmount += transaction.getTransaction().getTransactionAmount();
        maxAmount = transaction.getTransaction().getTransactionAmount() > maxAmount ?
                transaction.getTransaction().getTransactionAmount() : maxAmount;
        this.transactions.add(transaction);
        return  this;
    }

    public float getAvgAmount() {
        return this.totalAmount/this.noOfTransacions;
    }

    public Aggregated() {
    }

    @Override
    public String toString() {
        return "Aggregated{" +
                "avgAmount=" + avgAmount +
                ", totalAmount=" + totalAmount +
                ", noOfTransacions=" + noOfTransacions +
                ", maxAmount=" + maxAmount +
                '}';
    }
}
