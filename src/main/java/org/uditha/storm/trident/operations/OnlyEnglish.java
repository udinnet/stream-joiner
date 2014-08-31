package org.uditha.storm.trident.operations;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;
import twitter4j.User;


/**
 * @author uditha
 */
public class OnlyEnglish extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tuple) {
        User user = (User)tuple.get(0);

        return "en".equals(user.getLang());
    }
}
