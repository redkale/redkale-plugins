/*
 */
package org.redkalex.source.base;

import java.io.Serializable;
import org.redkale.convert.json.JsonConvert;
import org.redkale.persistence.*;
import org.redkale.source.*;

/**
 *
 * @author zhangjx
 */
@Entity
@DistributeTable(strategy = BeanRecord.TableStrategy.class)
public class BeanRecord {

    @Id
    private String recordsid = "";

    private short status;

    private String content = "";

    private long createTime;

    private byte[] img;

    public String getRecordsid() {
        return recordsid;
    }

    public void setRecordsid(String recordsid) {
        this.recordsid = recordsid;
    }

    public int imgLength() {
        return img == null ? -1 : img.length;
    }

    public short getStatus() {
        return status;
    }

    public void setStatus(short status) {
        this.status = status;
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

    public byte[] getImg() {
        return img;
    }

    public void setImg(byte[] img) {
        this.img = img;
    }

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }

    public static class TableStrategy implements DistributeTableStrategy<BeanRecord> {

        private static final String format = "%1$tY%1$tm";

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
        public String getTable(String table, BeanRecord bean) {
            return getSingleTable(table, bean.getCreateTime());
        }

        private String getSingleTable(String table, long createTime) {
            int pos = table.indexOf('.');
            return "" + table.substring(pos + 1) + "_" + String.format(format, createTime); //"notice."
        }

        @Override
        public String getTable(String table, Serializable primary) {
            String id = (String) primary;
            return getSingleTable(table, Long.parseLong(id.substring(0, id.indexOf('-'))));
        }
    }
}
