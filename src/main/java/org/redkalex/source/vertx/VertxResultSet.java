/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.vertx;

import io.vertx.core.buffer.Buffer;
import io.vertx.sqlclient.*;
import java.math.BigDecimal;
import java.util.*;
import org.redkale.annotation.Nullable;
import org.redkale.convert.ConvertDisabled;
import org.redkale.source.*;

/** @author zhangjx */
public class VertxResultSet implements DataResultSet {

    @Nullable
    private final EntityInfo info;

    private final RowIterator<Row> it;

    private final RowSet<Row> rowSet;

    private Row currentRow;

    public VertxResultSet(@Nullable EntityInfo info, RowSet<Row> rowSet) {
        this.info = info;
        this.rowSet = rowSet;
        this.it = rowSet == null ? null : rowSet.iterator();
    }

    @Override
    public boolean next() {
        if (it == null) {
            return false;
        }
        boolean has = it.hasNext();
        this.currentRow = has ? it.next() : null;
        return has;
    }

    @Override // 可以为空
    @ConvertDisabled
    public EntityInfo getEntityInfo() {
        return info;
    }

    @Override
    public void close() {
        // do nothing
    }

    // columnIndex从1开始
    @Override
    public Object getObject(int columnIndex) {
        return currentRow.getValue(columnIndex - 1);
    }

    @Override
    public Object getObject(String columnName) {
        int columnIndex = currentRow.getColumnIndex(columnName);
        if (columnIndex < 0) {
            columnIndex = currentRow.getColumnIndex(columnName.toLowerCase());
        }
        return currentRow.getValue(columnIndex);
    }

    @Override
    public String getString(int columnIndex) {
        return currentRow.getString(columnIndex - 1);
    }

    @Override
    public String getString(String columnLabel) {
        int columnIndex = currentRow.getColumnIndex(columnLabel);
        if (columnIndex < 0) {
            columnIndex = currentRow.getColumnIndex(columnLabel.toLowerCase());
        }
        return currentRow.getString(columnIndex);
    }

    @Override
    public byte[] getBytes(int columnIndex) {
        Buffer buffer = currentRow.getBuffer(columnIndex - 1);
        return buffer == null ? null : buffer.getBytes();
    }

    @Override
    public byte[] getBytes(String columnLabel) {
        int columnIndex = currentRow.getColumnIndex(columnLabel);
        if (columnIndex < 0) {
            columnIndex = currentRow.getColumnIndex(columnLabel.toLowerCase());
        }
        Buffer buffer = currentRow.getBuffer(columnIndex);
        return buffer == null ? null : buffer.getBytes();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) {
        return currentRow.getBigDecimal(columnIndex - 1);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) {
        int columnIndex = currentRow.getColumnIndex(columnLabel);
        if (columnIndex < 0) {
            columnIndex = currentRow.getColumnIndex(columnLabel.toLowerCase());
        }
        return currentRow.getBigDecimal(columnIndex);
    }

    @Override
    public Boolean getBoolean(int columnIndex) {
        return currentRow.getBoolean(columnIndex - 1);
    }

    @Override
    public Boolean getBoolean(String columnLabel) {
        int columnIndex = currentRow.getColumnIndex(columnLabel);
        if (columnIndex < 0) {
            columnIndex = currentRow.getColumnIndex(columnLabel.toLowerCase());
        }
        return currentRow.getBoolean(columnIndex);
    }

    @Override
    public Short getShort(int columnIndex) {
        return currentRow.getShort(columnIndex - 1);
    }

    @Override
    public Short getShort(String columnLabel) {
        int columnIndex = currentRow.getColumnIndex(columnLabel);
        if (columnIndex < 0) {
            columnIndex = currentRow.getColumnIndex(columnLabel.toLowerCase());
        }
        return currentRow.getShort(columnIndex);
    }

    @Override
    public Integer getInteger(int columnIndex) {
        return currentRow.getInteger(columnIndex - 1);
    }

    @Override
    public Integer getInteger(String columnLabel) {
        int columnIndex = currentRow.getColumnIndex(columnLabel);
        if (columnIndex < 0) {
            columnIndex = currentRow.getColumnIndex(columnLabel.toLowerCase());
        }
        return currentRow.getInteger(columnIndex);
    }

    @Override
    public Float getFloat(int columnIndex) {
        return currentRow.getFloat(columnIndex - 1);
    }

    @Override
    public Float getFloat(String columnLabel) {
        int columnIndex = currentRow.getColumnIndex(columnLabel);
        if (columnIndex < 0) {
            columnIndex = currentRow.getColumnIndex(columnLabel.toLowerCase());
        }
        return currentRow.getFloat(columnIndex);
    }

    @Override
    public Long getLong(int columnIndex) {
        return currentRow.getLong(columnIndex - 1);
    }

    @Override
    public Long getLong(String columnLabel) {
        int columnIndex = currentRow.getColumnIndex(columnLabel);
        if (columnIndex < 0) {
            columnIndex = currentRow.getColumnIndex(columnLabel.toLowerCase());
        }
        return currentRow.getLong(columnIndex);
    }

    @Override
    public Double getDouble(int columnIndex) {
        return currentRow.getDouble(columnIndex - 1);
    }

    @Override
    public Double getDouble(String columnLabel) {
        int columnIndex = currentRow.getColumnIndex(columnLabel);
        if (columnIndex < 0) {
            columnIndex = currentRow.getColumnIndex(columnLabel.toLowerCase());
        }
        return currentRow.getDouble(columnIndex);
    }

    @Override
    public boolean wasNull() {
        return currentRow == null;
    }

    @Override
    @ConvertDisabled
    public List<String> getColumnLabels() {
        return rowSet == null ? Collections.emptyList() : rowSet.columnsNames();
    }
}
