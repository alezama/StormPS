package com.example;

import com.bitso.Bitso;
import com.bitso.BitsoTicker;
import com.bitso.exceptions.BitsoAPIException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class YFSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Bitso bitso;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        bitso = new Bitso( "", "" );
    }

    public void nextTuple() {
        try {
            BitsoTicker[] tickers = bitso.getTicker();
            for (BitsoTicker ticker: tickers) {
                if (ticker.getBook().equalsIgnoreCase( "btc_mxn" )) {
                    collector.emit(new Values(ticker.getBook(),
                            ticker.getHigh(),
                            ticker.getLow(),
                            ticker.getCreatedAt()) );
                }
            }
        } catch (BitsoAPIException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare( new Fields("book", "high", "low", "created_at" ));
    }
}
