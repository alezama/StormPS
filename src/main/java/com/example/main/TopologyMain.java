package com.example.main;

import com.example.YFSpout;
import com.example.YTBolt;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout( "BitsoTickerSpout", new YFSpout() );
        builder.setBolt( "BitsoTickerBolt", new YTBolt() )
        .shuffleGrouping( "BitsoTickerSpout" );

        StormTopology topology = builder.createTopology();

        Config config = new Config();
        config.setDebug( true );
        config.put("fileToWrite", "~/bitso.txt");

        StormSubmitter.submitTopology( "BitsoTickerTopology", config, topology );
        try {
            Thread.sleep( 10000 );
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
