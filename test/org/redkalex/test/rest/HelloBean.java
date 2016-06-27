package org.redkalex.test.rest;

import org.redkale.convert.json.JsonFactory;
import org.redkale.source.FilterBean;


public class HelloBean implements FilterBean	{

    @Override
    public String toString() {
        return JsonFactory.root().getConvert().convertTo(this);
    }
}
