package org.uditha.storm.trident.operations;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;


/**
 * @author uditha
 */
public class Print extends BaseFilter {
    private int partitionIndex;
    private int numPartitions;
    private final String name;

    public Print(){
        name = "";
    }
    public Print(String name){
        this.name = name;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        this.partitionIndex = context.getPartitionIndex();
        this.numPartitions = context.numPartitions();
    }


    @Override
    public boolean isKeep(TridentTuple tuple) {
        System.err.println(String.format("%s::Partition idx: %s out of %s partitions got %s", name, partitionIndex, numPartitions, tuple.toString()));
        return true;
    }
}