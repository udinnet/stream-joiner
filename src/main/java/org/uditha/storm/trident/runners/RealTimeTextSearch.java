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
import org.uditha.storm.trident.runners.elasticsearch.ElasticSearchStateFactory;
import org.uditha.storm.trident.runners.elasticsearch.ElasticSearchStateUpdater;
import org.uditha.storm.trident.runners.elasticsearch.TweetQuery;
import org.uditha.storm.trident.operations.ParseTweet;
import org.uditha.storm.trident.operations.Print;
import org.uditha.storm.trident.operations.Split;
import org.uditha.storm.trident.operations.TweetIdExtractor;
import org.uditha.storm.trident.testutil.TestUtils;

import java.io.IOException;

/**
*
* @author uditha
*/
public class RealTimeTextSearch {

    public static StormTopology buildTopology(TransactionalTridentKafkaSpout spout)
            throws IOException {

        TridentTopology topology = new TridentTopology();
        topology
                .newStream("tweets", spout)
                .each(new Fields("str"), new ParseTweet(), new Fields("text", "content", "user"))
                .each(new Fields("text", "content"), new TweetIdExtractor(), new Fields("tweetId"))
                .project(new Fields("tweetId", "text"))
                .each(new Fields("tweetId", "text"), new Print())
                .partitionPersist(new ElasticSearchStateFactory(), new Fields("tweetId", "text"), new ElasticSearchStateUpdater());

        TridentState elasticSearchState = topology.newStaticState(new ElasticSearchStateFactory());
        topology
                .newDRPCStream("search")
                .each(new Fields("args"), new Split(" "), new Fields("keywords")) // let's split the arguments
                .stateQuery(elasticSearchState, new Fields("keywords"), new TweetQuery(), new Fields("ids")) // and pass them as query parameters
                .project(new Fields("ids"));
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
