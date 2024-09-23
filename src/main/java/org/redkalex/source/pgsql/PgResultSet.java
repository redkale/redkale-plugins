/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.io.*;
import java.math.BigDecimal;
import java.util.*;
import org.redkale.convert.ConvertDisabled;
import org.redkale.net.client.ClientResult;
import org.redkale.source.*;
import org.redkale.util.Attribute;

/** @author zhangjx */
@SuppressWarnings("deprecation")
public class PgResultSet implements DataResultSet, ClientResult {

    protected static final PgResultSet EMPTY = new PgResultSet() {
        {
            updateEffectCount = -1;
        }

        @Override
        public boolean next() {
            return false;
        }
    };

    protected PgClientRequest request;

    protected EntityInfo info;

    protected PgRowDesc rowDesc;

    protected Map<String, Integer> colmap;

    protected final List<PgRowData> rowData = new ArrayList<>(32);

    protected int rowIndex = -1;

    protected int limit = -1;

    protected int page = -1; // 一页的数据

    protected int[] pages; // 一页的数据

    protected int pageIndex;

    protected PgRowData currRow;

    protected int updateEffectCount;

    protected int effectRespCount;

    protected int[] batchEffectCounts;

    protected Object oneEntity; // 只有PgReqExtended.mode = FIND_ENTITY 才有效

    protected List<Object> listEntity; // 只有PgReqExtended.mode = FINDS_ENTITY/LISTALL_ENTITY 才有效

    public PgResultSet() {
        // do nothing
    }

    @Override // 可以为空
    @ConvertDisabled
    public EntityInfo getEntityInfo() {
        return info;
    }

    protected void prepare() {
        // do nothing
    }

    protected boolean recycle() {
        this.request = null;
        this.info = null;
        this.rowDesc = null;
        this.colmap = null;
        this.rowData.clear();
        this.rowIndex = -1;
        this.limit = -1;
        this.page = -1;
        this.pages = null;
        this.pageIndex = 0;
        this.currRow = null;
        this.updateEffectCount = 0;
        this.effectRespCount = 0;
        this.batchEffectCounts = null;
        this.oneEntity = null;
        this.listEntity = null;
        return true;
    }

    @Override
    public <T> Serializable getObject(Attribute<T, Serializable> attr, int index, String column) {
        if (currRow.realValues != null) {
            return index > 0 ? currRow.realValues[index - 1] : currRow.realValues[colmap().get(column.toLowerCase())];
        }
        return DataResultSet.getRowColumnValue(this, attr, index, column);
    }

    @Override
    public boolean isKeepAlive() {
        return true;
    }

    public void addEntity(Object entity) {
        if (listEntity == null) {
            listEntity = new ArrayList<>();
        }
        listEntity.add(entity);
    }

    @Override
    @ConvertDisabled
    public List<String> getColumnLabels() {
        List<String> labels = new ArrayList<>();
        for (PgRowColumn col : rowDesc.columns) {
            labels.add(col.getName());
        }
        return labels;
    }

    @Override
    public boolean wasNull() {
        return currRow == null;
    }

    @Override
    public Object getObject(int columnIndex) {
        return this.currRow.getObject(rowDesc, columnIndex - 1);
    }

    @Override
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
    public BigDecimal getBigDecimal(int columnIndex) {
        Object val = getObject(columnIndex);
        return val == null ? null : (BigDecimal) val;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) {
        Object val = getObject(columnLabel);
        return val == null ? null : (BigDecimal) val;
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
        for (PgRowColumn col : this.rowDesc.getColumns()) {
            colmap.put(col.getName().toLowerCase(), ++i);
        }
        return colmap;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "_" + Objects.hashCode(this)
                + "{\"rowDesc\":" + rowDesc
                + ", \"rowTable\":" + (request == null || request.info == null ? null : request.info.getOriginTable())
                + ", \"rowData_size\":" + rowData.size()
                + ", \"updateEffectCount\":" + updateEffectCount
                + ", \"hashCode\": " + Objects.hashCode(this)
                + "}";
    }

    public PgResultSet increUpdateEffectCount(int c) {
        updateEffectCount += c;
        return this;
    }

    public PgResultSet increBatchEffectCount(int length, int c) {
        if (batchEffectCounts == null) {
            batchEffectCounts = new int[length];
        }
        batchEffectCounts[effectRespCount] = c;
        return this;
    }

    public int[] getBatchEffectCounts() {
        return batchEffectCounts;
    }

    public int getUpdateEffectCount() {
        return updateEffectCount;
    }

    public void setUpdateEffectCount(int updateEffectCount) {
        this.updateEffectCount = updateEffectCount;
    }

    public void setRowDesc(PgRowDesc rowDesc) {
        this.rowDesc = rowDesc;
    }

    public PgResultSet addRowData(PgRowData rowData) {
        this.rowData.add(rowData);
        return this;
    }

    @ConvertDisabled
    public long getLongValue(int columnIndex) {
        return ((Number) this.currRow.getObject(rowDesc, columnIndex - 1)).longValue();
    }

    @Override
    public boolean next() {
        if (this.rowData.isEmpty()) {
            return false;
        }
        if (++this.rowIndex < (this.limit > 0 ? this.limit : this.rowData.size())) {
            this.currRow = this.rowData.get(this.rowIndex);
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        if (page > 0) {
            this.limit += this.page;
            this.rowIndex--; // merge模式下需要-1
        } else if (pages != null && pageIndex < pages.length - 1) {
            this.limit += this.pages[++pageIndex];
            this.rowIndex--; // merge模式下需要-1
        }
    }
}
