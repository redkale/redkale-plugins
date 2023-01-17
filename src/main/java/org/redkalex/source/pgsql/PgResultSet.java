/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import org.redkale.convert.ConvertDisabled;
import org.redkale.source.*;
import org.redkale.util.Attribute;

/**
 *
 * @author zhangjx
 */
@SuppressWarnings("deprecation")
public class PgResultSet implements java.sql.ResultSet, DataResultSet {

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

    protected int page = -1; //一页的数据

    protected int[] pages; //一页的数据

    protected int pageIndex;

    protected PgRowData currRow;

    protected int updateEffectCount;

    protected int effectRespCount;

    protected int[] batchEffectCounts;

    protected Object oneEntity; //只有PgReqExtended.mode = FIND_ENTITY 才有效

    protected List<Object> listEntity; //只有PgReqExtended.mode = FINDS_ENTITY/LISTALL_ENTITY 才有效

    public PgResultSet() {
    }

    @Override //可以为空
    @ConvertDisabled
    public EntityInfo getEntityInfo() {
        return info;
    }

    protected void prepare() {
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

    public void addEntity(Object entity) {
        if (listEntity == null) {
            listEntity = new ArrayList<>();
        }
        listEntity.add(entity);
    }

    @Override
    @ConvertDisabled
    public Object getObject(int columnIndex) {
        return this.currRow.getObject(rowDesc, columnIndex - 1);
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
    @ConvertDisabled
    public Object getObject(String columnLabel) {
        Integer index = colmap().get(columnLabel.toLowerCase());
        if (index == null) {
            throw new SourceException("not found column " + columnLabel + " index in " + colmap);
        }
        return this.currRow.getObject(rowDesc, index);
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
        return getClass().getSimpleName() + "_" + Objects.hashCode(this) + "{\"rowDesc\":" + rowDesc + ", \"rowTable\":" + (request == null || request.info == null ? null : request.info.getOriginTable()) + ", \"rowData_size\":" + (rowData == null ? -1 : rowData.size()) + ", \"updateEffectCount\":" + updateEffectCount + ", \"hashCode\": " + Objects.hashCode(this) + "}";
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
            this.rowIndex--; //merge模式下需要-1
        } else if (pages != null && pageIndex < pages.length - 1) {
            this.limit += this.pages[++pageIndex];
            this.rowIndex--; //merge模式下需要-1
        }
    }

    @Override
    @ConvertDisabled
    public boolean wasNull() {
        return currRow == null;
    }

    @Override
    @ConvertDisabled
    public String getString(int columnIndex) throws SQLException {
        return String.valueOf(this.currRow.getObject(rowDesc, columnIndex - 1));
    }

    @Override
    @ConvertDisabled
    public boolean getBoolean(int columnIndex) throws SQLException {
        return (Boolean) this.currRow.getObject(rowDesc, columnIndex - 1);
    }

    @Override
    @ConvertDisabled
    public byte getByte(int columnIndex) throws SQLException {
        return (Byte) this.currRow.getObject(rowDesc, columnIndex - 1);
    }

    @Override
    @ConvertDisabled
    public short getShort(int columnIndex) throws SQLException {
        return ((Number) this.currRow.getObject(rowDesc, columnIndex - 1)).shortValue();
    }

    @Override
    @ConvertDisabled
    public int getInt(int columnIndex) throws SQLException {
        return ((Number) this.currRow.getObject(rowDesc, columnIndex - 1)).intValue();
    }

    @Override
    @ConvertDisabled
    public long getLong(int columnIndex) throws SQLException {
        return ((Number) this.currRow.getObject(rowDesc, columnIndex - 1)).longValue();
    }

    @Override
    @ConvertDisabled
    public float getFloat(int columnIndex) throws SQLException {
        return ((Number) this.currRow.getObject(rowDesc, columnIndex - 1)).floatValue();
    }

    @Override
    @ConvertDisabled
    public double getDouble(int columnIndex) throws SQLException {
        return ((Number) this.currRow.getObject(rowDesc, columnIndex - 1)).doubleValue();
    }

    @Override
    @ConvertDisabled
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return (BigDecimal) this.currRow.getObject(rowDesc, columnIndex - 1);
    }

    @Override
    @ConvertDisabled
    public byte[] getBytes(int columnIndex) throws SQLException {
        return (byte[]) this.currRow.getObject(rowDesc, columnIndex - 1);
    }

    @Override
    @ConvertDisabled
    public Date getDate(int columnIndex) throws SQLException {
        return (Date) this.currRow.getObject(rowDesc, columnIndex - 1);
    }

    @Override
    @ConvertDisabled
    public Time getTime(int columnIndex) throws SQLException {
        return (Time) this.currRow.getObject(rowDesc, columnIndex - 1);
    }

    @Override
    @ConvertDisabled
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return (Timestamp) this.currRow.getObject(rowDesc, columnIndex - 1);
    }

    @Override
    @ConvertDisabled
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    @Deprecated
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public String getString(String columnLabel) throws SQLException {
        return String.valueOf(this.currRow.getObject(rowDesc, colmap().get(columnLabel.toLowerCase())));
    }

    @Override
    @ConvertDisabled
    public boolean getBoolean(String columnLabel) throws SQLException {
        return (Boolean) this.currRow.getObject(rowDesc, colmap().get(columnLabel.toLowerCase()));
    }

    @Override
    @ConvertDisabled
    public byte getByte(String columnLabel) throws SQLException {
        return (Byte) this.currRow.getObject(rowDesc, colmap().get(columnLabel.toLowerCase()));
    }

    @Override
    @ConvertDisabled
    public short getShort(String columnLabel) throws SQLException {
        return ((Number) this.currRow.getObject(rowDesc, colmap().get(columnLabel.toLowerCase()))).shortValue();
    }

    @Override
    @ConvertDisabled
    public int getInt(String columnLabel) throws SQLException {
        return ((Number) this.currRow.getObject(rowDesc, colmap().get(columnLabel.toLowerCase()))).intValue();
    }

    @Override
    @ConvertDisabled
    public long getLong(String columnLabel) throws SQLException {
        return ((Number) this.currRow.getObject(rowDesc, colmap().get(columnLabel.toLowerCase()))).longValue();
    }

    @Override
    @ConvertDisabled
    public float getFloat(String columnLabel) throws SQLException {
        return ((Number) this.currRow.getObject(rowDesc, colmap().get(columnLabel.toLowerCase()))).floatValue();
    }

    @Override
    @ConvertDisabled
    public double getDouble(String columnLabel) throws SQLException {
        return ((Number) this.currRow.getObject(rowDesc, colmap().get(columnLabel.toLowerCase()))).doubleValue();
    }

    @Override
    @ConvertDisabled
    @Deprecated
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return (BigDecimal) this.currRow.getObject(rowDesc, colmap().get(columnLabel.toLowerCase()));
    }

    @Override
    @ConvertDisabled
    public byte[] getBytes(String columnLabel) throws SQLException {
        return (byte[]) this.currRow.getObject(rowDesc, colmap().get(columnLabel.toLowerCase()));
    }

    @Override
    @ConvertDisabled
    public Date getDate(String columnLabel) throws SQLException {
        return (Date) this.currRow.getObject(rowDesc, colmap().get(columnLabel.toLowerCase()));
    }

    @Override
    @ConvertDisabled
    public Time getTime(String columnLabel) throws SQLException {
        return (Time) this.currRow.getObject(rowDesc, colmap().get(columnLabel.toLowerCase()));
    }

    @Override
    @ConvertDisabled
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return (Timestamp) this.currRow.getObject(rowDesc, colmap().get(columnLabel.toLowerCase()));
    }

    @Override
    @ConvertDisabled
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    @Deprecated
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public String getCursorName() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    @ConvertDisabled
    public int findColumn(String columnLabel) throws SQLException {
        return colmap().get(columnLabel.toLowerCase()) + 1;
    }

    @Override
    @ConvertDisabled
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return (BigDecimal) this.currRow.getObject(rowDesc, columnIndex - 1);
    }

    @Override
    @ConvertDisabled
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return (BigDecimal) this.currRow.getObject(rowDesc, colmap().get(columnLabel.toLowerCase()));
    }

    @Override
    @ConvertDisabled
    public boolean isBeforeFirst() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public boolean isAfterLast() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public boolean isFirst() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public boolean isLast() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void beforeFirst() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void afterLast() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public boolean first() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public boolean last() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public int getRow() throws SQLException {
        return this.rowData.size();
    }

    @Override
    @ConvertDisabled
    public boolean absolute(int row) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public boolean relative(int rows) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public boolean previous() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void setFetchSize(int rows) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public int getFetchSize() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public int getType() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public int getConcurrency() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public boolean rowUpdated() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public boolean rowInserted() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public boolean rowDeleted() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateNull(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateNull(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void insertRow() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateRow() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void deleteRow() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void refreshRow() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void cancelRowUpdates() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void moveToInsertRow() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void moveToCurrentRow() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Statement getStatement() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Ref getRef(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Clob getClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Array getArray(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Ref getRef(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Clob getClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Array getArray(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public URL getURL(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public URL getURL(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public String getNString(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public String getNString(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    @ConvertDisabled
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
