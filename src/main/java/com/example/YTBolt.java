package com.example;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.PrintWriter;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class YTBolt extends BaseBasicBolt {

    private PrintWriter writer;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare( stormConf, context );
        String fileName = stormConf.get( "fileToWrite" ).toString();
        try {
            this.writer = new PrintWriter( fileName, "UTF-8" );
        } catch (Exception e) {
            throw new RuntimeException("Error opening the file ["+fileName+"]");
        }
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        String book = tuple.getValue( 0 ).toString();
        BigDecimal high = (BigDecimal) tuple.getValue( 1 );
        BigDecimal low = (BigDecimal) tuple.getValue( 2 );
        BigDecimal difference = high.subtract( low );
        basicOutputCollector.emit( new Values(book, tuple.getValue( 3 ), difference) );
        writer.println(book + " " + sdf.format( (Date)tuple.getValue( 3 ) ) + " difference: " + difference );
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare( new Fields( "book", "created_at", "difference" ) );
    }

    @Override
    public void cleanup() {
        super.cleanup();
        this.writer.close();
    }
}
