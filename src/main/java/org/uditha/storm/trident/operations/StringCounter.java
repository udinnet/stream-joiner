package org.uditha.storm.trident.operations;

import backtype.storm.tuple.Values;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/**
* @author uditha
*/
public class StringCounter implements Aggregator<Map<String, Integer>> {

    private int partitionId;
    private int numPartitions;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        this.partitionId = context.getPartitionIndex();
        this.numPartitions = context.numPartitions();
    }

    @Override
    public void cleanup() {
    }

    @Override
    public Map<String, Integer> init(Object batchId, TridentCollector collector) {
        return new HashMap<String, Integer>();
    }

    @Override
    public void aggregate(Map<String, Integer> val, TridentTuple tuple, TridentCollector collector) {
        String loc = tuple.getString(0);
        Integer previousValue = val.get(loc);
        previousValue = previousValue == null ? 0 : previousValue;
        val.put(loc, previousValue + 1);
    }

    @Override
    public void complete(Map<String, Integer> val, TridentCollector collector) {
        System.err.println(String.format("Partition %s out ot %s partitions aggregated:%s", partitionId, numPartitions, val));
        collector.emit(new Values(val));
    }
}

