package org.uditha.storm.trident.operations;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import org.uditha.storm.trident.testutil.Content;
import twitter4j.Status;


/**
 * @author uditha
 */
public class ExtractLocation extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Status status = (Status) tuple.get(0);
        Content content = (Content) tuple.get(1);

        collector.emit(new Values(status.getPlace().getCountryCode(), content.getContentName()));
    }
}
