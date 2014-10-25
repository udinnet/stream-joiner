package org.uditha.storm.trident.runners;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FirstN;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.TupleCollectionGet;
import storm.trident.testing.MemoryMapState;
import org.uditha.storm.trident.operations.ExtractLocation;
import org.uditha.storm.trident.operations.OnlyGeo;
import org.uditha.storm.trident.operations.OnlyHashtags;
import org.uditha.storm.trident.operations.ParseTweet;
import org.uditha.storm.trident.testutil.TestUtils;

import java.io.IOException;

/**
* @author uditha
*/
public class TopHashtagByCountry {

    public static StormTopology buildTopology(TransactionalTridentKafkaSpout spout) throws IOException {

        TridentTopology topology = new TridentTopology();
        TridentState count =
        topology
                .newStream("tweets", spout)
                .each(new Fields("str"), new ParseTweet(), new Fields("status", "content", "user"))
                .project(new Fields("content", "user", "status"))
                .each(new Fields("content"), new OnlyHashtags())
                .each(new Fields("status"), new OnlyGeo())
                .each(new Fields("status", "content"), new ExtractLocation(), new Fields("country", "contentName"))
                .groupBy(new Fields("country", "contentName"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
        ;


        topology
                .newDRPCStream("location_hashtag_count")
                .stateQuery(count, new TupleCollectionGet(), new Fields("country", "contentName"))
                .stateQuery(count, new Fields("country", "contentName"), new MapGet(), new Fields("count"))
                .groupBy(new Fields("country"))
                .aggregate(new Fields("contentName", "count"), new FirstN.FirstNSortedAgg(3,"count", true), new Fields("contentName", "count"))
        ;

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
