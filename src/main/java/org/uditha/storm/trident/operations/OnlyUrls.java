package org.uditha.storm.trident.operations;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;
import org.uditha.storm.trident.testutil.Content;


/**
 * @author uditha
 */
public class OnlyUrls extends BaseFilter {

    @Override
    public boolean isKeep(TridentTuple tuple) {
        Content content = (Content) tuple.getValueByField("content");
        return "url".equals(content.getContentType());
    }
}
