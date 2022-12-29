/*
 *
 */
package org.redkalex.source.mysql;

import java.io.Serializable;
import java.util.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.persistence.*;
import org.redkale.source.*;

/**
 *
 * @author zhangjx
 */
@Entity
@DistributeTable(strategy = DayRecord.TableStrategy.class)
public class DayRecord {

    @Id
    @Column(length = 64, comment = "主键")
    private String recordid = "";

    @Column(length = 60, comment = "内容")
    private String content = "";

    @Column(updatable = false, comment = "生成时间，单位毫秒")
    private long createTime;

    public String getRecordid() {
        return recordid;
    }

    public void setRecordid(String recordid) {
        this.recordid = recordid;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }

    public static class TableStrategy implements DistributeTableStrategy<DayRecord> {

        private static final String format = "%1$tY%1$tm%1$td";

        @Override
        public String[] getTables(String table, FilterNode node) {
            Object id = node.findValue("recordid");
            if (id != null) {
                if (id instanceof String) {
                    return new String[]{getTable(table, (Serializable) id)};
                } else if (id instanceof Collection) {
                    Set<String> tables = new LinkedHashSet<>();
                    for (String i : (Collection<String>) id) {
                        tables.add(getTable(table, (Serializable) i));
                    }
                    return tables.toArray(new String[tables.size()]);
                } else if (id instanceof String[]) {
                    Set<String> tables = new LinkedHashSet<>();
                    for (String i : (String[]) id) {
                        tables.add(getTable(table, (Serializable) i));
                    }
                    return tables.toArray(new String[tables.size()]);
                } else {
                    throw new SourceException(DayRecord.class.getSimpleName() + ".TableStrategy not supported filter node: " + node);
                }
            }
            Object time = node.findValue("createTime");
            if (time == null) {
                time = node.findValue("#createTime");
            }
            if (time instanceof Long) {
                return new String[]{getSingleTable(table, (Long) time)};
            }
            Range.LongRange createTime = (Range.LongRange) time;
            if (createTime.getMax() != null && createTime.getMax() != Long.MAX_VALUE
                && createTime.getMax() > createTime.getMin()) {
                List<String> tables = new ArrayList<>();
                long start = createTime.getMin();
                while (start < createTime.getMax()) {
                    tables.add(getSingleTable(table, start));
                    start += 24 * 60 * 60 * 1000L;
                }
                return tables.toArray(new String[tables.size()]);
            }
            return new String[]{getSingleTable(table, createTime.getMin())};
        }

        @Override
        public String getTable(String table, DayRecord bean) {
            return getSingleTable(table, bean.getCreateTime());
        }

        private String getSingleTable(String table, long createTime) {
            int pos = table.indexOf('.');
            String nt = table.substring(pos + 1) + "_" + String.format(format, createTime);
            //return "aa_test_" + createTime % 4 + "." + nt;
            return nt;
        }

        @Override
        public String getTable(String table, Serializable primary) {
            String id = (String) primary;
            return getSingleTable(table, Long.parseLong(id.substring(id.indexOf('-') + 1)));
        }
    }
}
