/*
 *
 */
package org.redkalex.source.parser;

import java.util.Map;
import org.redkale.convert.json.JsonConvert;

/**
 *
 * @author zhangjx
 */
public class NativeSqlTemplet {

    private String templetSql;

    private Map<String, Object> templetParams;

    public NativeSqlTemplet() {
        //do nothing
    }

    public NativeSqlTemplet(String templetSql, Map<String, Object> templetParams) {
        this.templetSql = templetSql;
        this.templetParams = templetParams;
    }

    public String getTempletSql() {
        return templetSql;
    }

    public void setTempletSql(String templetSql) {
        this.templetSql = templetSql;
    }

    public Map<String, Object> getTempletParams() {
        return templetParams;
    }

    public void setTempletParams(Map<String, Object> templetParams) {
        this.templetParams = templetParams;
    }

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }

}
