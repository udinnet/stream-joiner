package org.uditha.storm.trident;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.trident.TridentTopology;
import org.uditha.storm.trident.operations.*;
import org.uditha.storm.trident.testutil.TestUtils;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;


/**
* @author uditha
*/
public class Skeleton {
    private static final Logger log = LoggerFactory.getLogger(Skeleton.class);
    public static StormTopology buildTopology(TransactionalTridentKafkaSpout spout) throws IOException {
        TridentTopology topology = new TridentTopology();
        topology
                .newStream("tweets", spout)
                .each(new Fields("str"), new Print())
        ;

        topology
                .newDRPCStream("ping");

        return topology.build();
    }


    public static void main(String[] args) throws Exception {
        Config conf = new Config();


        if (args.length == 2) {
            // Ready & submit the topology
            String name = args[0];
            BrokerHosts hosts = new ZkHosts(args[1]);
            TransactionalTridentKafkaSpout kafkaSpout = TestUtils.testTweetSpout(hosts);

            StormSubmitter.submitTopology(name, conf, buildTopology(kafkaSpout));

        }else{
            System.err.println("<topologyName> <zookeeperHost>");
        }

    }


}
