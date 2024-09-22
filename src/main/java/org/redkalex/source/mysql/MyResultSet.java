/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.util.*;
import org.redkale.convert.ConvertDisabled;
import org.redkale.net.WorkThread;
import org.redkale.net.client.ClientResult;
import org.redkale.source.*;
import org.redkale.util.ObjectPool;

/** @author zhangjx */
@SuppressWarnings("deprecation")
public class MyResultSet implements DataResultSet, ClientResult {

    protected static final MyResultSet EMPTY = new MyResultSet() {
        {
            updateEffectCount = -1;
        }

        @Override
        public boolean next() {
            return false;
        }
    };

    protected static final short STATUS_QUERY_ROWDESC = 11;

    protected static final short STATUS_QUERY_ROWDATA = 12;

    protected static final short STATUS_PREPARE_PARAM = 21;

    protected static final short STATUS_PREPARE_COLUMN = 22;

    protected static final short STATUS_EXTEND_COLUMN = 31;

    protected static final short STATUS_EXTEND_ROWDATA = 32;

    protected final WorkThread thread = WorkThread.currentWorkThread();

    protected EntityInfo info;

    protected short status;

    int rowColumnDecodeIndex = -1;

    // ------------------------------------------------------------
    MyRespPrepare prepare; // 只会在prepare命令时此字段才会有值

    // ------------------------------------------------------------
    protected ObjectPool<MyResultSet> objpool;

    protected MyClientRequest request;

    protected MyRowDesc rowDesc;

    protected Map<String, Integer> colmap;

    protected final List<MyRowData> rowData = new ArrayList<>(32);

    protected int rowIndex = -1;

    protected MyRowData currRow;

    protected int updateEffectCount;

    protected int effectRespCount;

    protected int[] batchEffectCounts;

    public int getUpdateEffectCount() {
        return this.updateEffectCount;
    }

    public int[] getBatchEffectCounts() {
        return this.batchEffectCounts;
    }

    @Override // 可以为空
    @ConvertDisabled
    public EntityInfo getEntityInfo() {
        return info;
    }

    @Override
    public boolean isKeepAlive() {
        return true;
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "_" + Objects.hashCode(this) + "{\"rowDesc\":" + rowDesc + ", \"rowTable\":"
                + (request == null || request.info == null ? null : request.info.getOriginTable())
                + ", \"rowDataSize\":" + (rowData == null ? -1 : rowData.size()) + ", \"updateEffectCount\":"
                + updateEffectCount + "}";
    }

    public MyResultSet increUpdateEffectCount(int c) {
        updateEffectCount += c;
        return this;
    }

    public MyResultSet increBatchEffectCount(int length, int c) {
        if (batchEffectCounts == null) {
            batchEffectCounts = new int[length];
        }
        batchEffectCounts[effectRespCount] = c;
        return this;
    }

    public void setUpdateEffectCount(int updateEffectCount) {
        this.updateEffectCount = updateEffectCount;
    }

    public void setRowDesc(MyRowDesc rowDesc) {
        this.rowDesc = rowDesc;
    }

    public MyResultSet addRowData(MyRowData rowData) {
        this.rowData.add(rowData);
        return this;
    }

    @Override
    public boolean next() {
        if (this.rowData.isEmpty()) {
            return false;
        }
        if (++this.rowIndex < this.rowData.size()) {
            this.currRow = this.rowData.get(this.rowIndex);
            return true;
        }
        return false;
    }

    @Override
    @ConvertDisabled
    public List<String> getColumnLabels() {
        List<String> labels = new ArrayList<>();
        for (MyRowColumn col : rowDesc.columns) {
            labels.add(col.columnLabel);
        }
        return labels;
    }

    @Override
    @ConvertDisabled
    public boolean wasNull() {
        return this.currRow == null;
    }

    @Override
    @ConvertDisabled
    public Object getObject(int columnIndex) {
        return this.currRow.getObject(rowDesc, columnIndex - 1);
    }

    @Override
    @ConvertDisabled
    public Object getObject(String columnLabel) {
        Integer index = colmap().get(columnLabel.toLowerCase());
        if (index == null) {
            throw new SourceException("not found column " + columnLabel + " index in " + colmap);
        }
        return this.currRow.getObject(rowDesc, index);
    }

    @Override
    public String getString(int columnIndex) {
        Object val = getObject(columnIndex);
        return val == null ? null : val.toString();
    }

    @Override
    public String getString(String columnLabel) {
        Object val = getObject(columnLabel);
        return val == null ? null : val.toString();
    }

    @Override
    public byte[] getBytes(int columnIndex) {
        Object val = getObject(columnIndex);
        return val == null ? null : (byte[]) val;
    }

    @Override
    public byte[] getBytes(String columnLabel) {
        Object val = getObject(columnLabel);
        return val == null ? null : (byte[]) val;
    }

    @Override
    public Boolean getBoolean(int columnIndex) {
        Object val = getObject(columnIndex);
        return val == null ? null : (Boolean) val;
    }

    @Override
    public Boolean getBoolean(String columnLabel) {
        Object val = getObject(columnLabel);
        return val == null ? null : (Boolean) val;
    }

    @Override
    public Short getShort(int columnIndex) {
        Object val = getObject(columnIndex);
        return val == null ? null : ((Number) val).shortValue();
    }

    @Override
    public Short getShort(String columnLabel) {
        Object val = getObject(columnLabel);
        return val == null ? null : ((Number) val).shortValue();
    }

    @Override
    public Integer getInteger(int columnIndex) {
        Object val = getObject(columnIndex);
        return val == null ? null : ((Number) val).intValue();
    }

    @Override
    public Integer getInteger(String columnLabel) {
        Object val = getObject(columnLabel);
        return val == null ? null : ((Number) val).intValue();
    }

    @Override
    public Float getFloat(int columnIndex) {
        Object val = getObject(columnIndex);
        return val == null ? null : ((Number) val).floatValue();
    }

    @Override
    public Float getFloat(String columnLabel) {
        Object val = getObject(columnLabel);
        return val == null ? null : ((Number) val).floatValue();
    }

    @Override
    public Long getLong(int columnIndex) {
        Object val = getObject(columnIndex);
        return val == null ? null : ((Number) val).longValue();
    }

    @Override
    public Long getLong(String columnLabel) {
        Object val = getObject(columnLabel);
        return val == null ? null : ((Number) val).longValue();
    }

    @Override
    public Double getDouble(int columnIndex) {
        Object val = getObject(columnIndex);
        return val == null ? null : ((Number) val).doubleValue();
    }

    @Override
    public Double getDouble(String columnLabel) {
        Object val = getObject(columnLabel);
        return val == null ? null : ((Number) val).doubleValue();
    }

    private Map<String, Integer> colmap() {
        if (colmap != null) {
            return colmap;
        }
        colmap = new HashMap<>();
        int i = -1;
        for (MyRowColumn col : this.rowDesc.getColumns()) {
            colmap.put(col.columnLabel.toLowerCase(), ++i);
        }
        return colmap;
    }
}
