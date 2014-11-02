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
import org.uditha.storm.trident.operations.ExtractFollowerClassAndContentName;
import org.uditha.storm.trident.operations.OnlyEnglish;
import org.uditha.storm.trident.operations.OnlyHashtags;
import org.uditha.storm.trident.operations.ParseTweet;
import org.uditha.storm.trident.testutil.TestUtils;

import java.io.IOException;

/**
* @author uditha
*/
public class TopHashtagByFollowerClass {

    public static StormTopology buildTopology(TransactionalTridentKafkaSpout spout) throws IOException {

        TridentTopology topology = new TridentTopology();
        TridentState count =
        topology
                .newStream("tweets", spout)
                .each(new Fields("str"), new ParseTweet(), new Fields("text", "content", "user"))
                .project(new Fields("content", "user"))
                .each(new Fields("content"), new OnlyHashtags())
                .each(new Fields("user"), new OnlyEnglish())
                .each(new Fields("content", "user"), new ExtractFollowerClassAndContentName(), new Fields("followerClass", "contentName"))
                .groupBy(new Fields("followerClass", "contentName"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
        ;


        topology
                .newDRPCStream("hashtag_count")
                .stateQuery(count, new TupleCollectionGet(), new Fields("followerClass", "contentName"))
                .stateQuery(count, new Fields("followerClass", "contentName"), new MapGet(), new Fields("count"))
                .groupBy(new Fields("followerClass"))
                .aggregate(new Fields("contentName", "count"), new FirstN.FirstNSortedAgg(1,"count", true), new Fields("contentName", "count"))
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
