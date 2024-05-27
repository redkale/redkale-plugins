/*
 *
 */
package org.redkalex.source.parser;

import java.util.Map;
import org.redkale.convert.json.JsonConvert;

/** @author zhangjx */
public class NativeSqlTemplet {

    private String jdbcSql;

    private Map<String, Object> templetParams;

    public NativeSqlTemplet() {
        // do nothing
    }

    public NativeSqlTemplet(String templetSql, Map<String, Object> templetParams) {
        this.jdbcSql = templetSql;
        this.templetParams = templetParams;
    }

    public String getJdbcSql() {
        return jdbcSql;
    }

    public void setJdbcSql(String jdbcSql) {
        this.jdbcSql = jdbcSql;
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
