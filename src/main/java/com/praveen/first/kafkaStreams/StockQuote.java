package com.praveen.first.kafkaStreams;


import java.sql.Timestamp;

public class StockQuote {

    public String getSymbol() {
        return symbol;
    }

    public Float getPrice() {
        return price;
    }

    public Float getVolume() {
        return volume;
    }

    public Float getHigh() {
        return high;
    }

    public Float getLow() {
        return low;
    }

    public String getTime() {
        return time;
    }

    private String symbol;
    private Float price;
    private Float volume;
    private Float high;
    private Float low;
    private String time;

    public StockQuote() {
    }

    public StockQuote(String symbol, Float price, Float volume, Float high, Float low, String time) {
        this.symbol = symbol;
        this.price = price;
        this.volume = volume;
        this.high = high;
        this.low = low;
        this.time = time;
    }
}
