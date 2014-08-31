package org.uditha.storm.trident.operations;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;
import twitter4j.Status;


/**
 * @author uditha
 */
public class OnlyGeo extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tuple) {
        Status status = (Status) tuple.get(0);
        return !(null == status.getPlace() || null == status.getPlace().getCountryCode());
    }
}
