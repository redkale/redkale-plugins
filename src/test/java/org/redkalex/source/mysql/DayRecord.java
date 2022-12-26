/*
 *
 */
package org.redkalex.source.mysql;

import java.io.Serializable;
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

    @Column(length = 1024, comment = "内容")
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

    public static class TableStrategy implements DistributeTableStrategy<DayRecord> {

        private static final String format = "%1$tY%1$tm%1$td";

        @Override
        public String[] getTables(String table, FilterNode node) {
            Object time = node.findValue("createTime");
            if (time == null) time = node.findValue("#createTime");
            if (time instanceof Long) {
                return new String[]{getSingleTable(table, (Long) time)};
            }
            Range.LongRange createTime = (Range.LongRange) time;
            return new String[]{getSingleTable(table, createTime.getMin())};
        }

        @Override
        public String getTable(String table, DayRecord bean) {
            return getSingleTable(table, bean.getCreateTime());
        }

        private String getSingleTable(String table, long createTime) {
            int pos = table.indexOf('.');
            return "" + table.substring(pos + 1) + "_" + String.format(format, createTime);
        }

        @Override
        public String getTable(String table, Serializable primary) {
            String id = (String) primary;
            return getSingleTable(table, Long.parseLong(id.substring(0, id.indexOf('-'))));
        }
    }
}
