package org.uditha.storm.trident.operations;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import org.uditha.storm.trident.testutil.Content;

/**
 * @author uditha
 */
public class TweetIdExtractor extends BaseFunction {

    @Override
    public void execute(TridentTuple objects, TridentCollector tridentCollector) {
        Content content = (Content) objects.getValueByField("content");
        tridentCollector.emit(new Values(content.getTweetId()));
    }
}
