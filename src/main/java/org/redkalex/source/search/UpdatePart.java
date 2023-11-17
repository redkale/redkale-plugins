/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.search;

import java.io.Serializable;
import java.util.*;
import org.redkale.source.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class UpdatePart extends BaseBean {

    public Object doc;

    public UpdateScript script;

    public UpdatePart() {
    }

    public UpdatePart(SearchInfo info, Object entity) {
        this.doc = entity;
    }

    public UpdatePart(SearchInfo info, Object entity, SelectColumn selects) {
        Map<String, Object> map = new HashMap<>();
        for (Attribute attr : info.getUpdateAttributes()) {
            if (selects == null || selects.test(attr.field())) {
                map.put(attr.field(), attr.get(entity));
            }
        }
        this.doc = map;
    }

    public UpdatePart(SearchInfo info, String column, Serializable value) {
        this.doc = Utility.ofMap(column, value);
    }

    public UpdatePart(SearchInfo info, ColumnValue... values) {
        boolean onlymov = true;
        for (ColumnValue cv : values) {
            if (cv.getExpress() != ColumnExpress.MOV) {
                onlymov = false;
                break;
            }
        }
        if (onlymov) {
            Map<String, Object> map = new HashMap<>();
            for (ColumnValue cv : values) {
                if (!(cv.getValue() instanceof ColumnNumberNode) && !(cv.getValue() instanceof ColumnNameNode)) {
                    throw new IllegalArgumentException("Not supported ColumnValue " + cv.getValue());
                }
                map.put(cv.getColumn(), getColumnOrNumberValue(cv.getValue()));
            }
            this.doc = map;
        } else {
            UpdateScript us = new UpdateScript();
            StringBuilder sb = new StringBuilder();
            Map<String, Serializable> map = new LinkedHashMap<>();
            for (ColumnValue cv : values) {
                if (!(cv.getValue() instanceof ColumnNumberNode) && !(cv.getValue() instanceof ColumnNameNode)) {
                    throw new IllegalArgumentException("Not supported ColumnValue " + cv.getValue());
                }
                build(sb, cv);
                map.put(cv.getColumn(), getColumnOrNumberValue(cv.getValue()));
            }
            us.source = sb.toString();
            us.params = map;
            this.script = us;
        }
    }

    protected Serializable getColumnOrNumberValue(ColumnNode node) {
        if (node instanceof ColumnNumberNode) {
            return ((ColumnNumberNode) node).getValue();
        } else if (node instanceof ColumnNameNode) {
            return ((ColumnNameNode) node).getColumn();
        }
        throw new IllegalArgumentException("Not supported ColumnValue " + node);
    }

    private void build(StringBuilder sb, ColumnValue val) {
        switch (val.getExpress()) {
            case MOV:
                sb.append("ctx._source.").append(val.getColumn()).append(" = params.").append(val.getColumn()).append(";");
                break;
            case INC:
                sb.append("ctx._source.").append(val.getColumn()).append(" += params.").append(val.getColumn()).append(";");
                break;
            case DEC:
                sb.append("ctx._source.").append(val.getColumn()).append(" -= params.").append(val.getColumn()).append(";");
                break;
            case MUL:
                sb.append("ctx._source.").append(val.getColumn()).append(" *= params.").append(val.getColumn()).append(";");
                break;
            case DIV:
                sb.append("ctx._source.").append(val.getColumn()).append(" /= params.").append(val.getColumn()).append(";");
                break;
            case MOD:
                sb.append("ctx._source.").append(val.getColumn()).append(" %= params.").append(val.getColumn()).append(";");
                break;
            case AND:
                sb.append("ctx._source.").append(val.getColumn()).append(" &= params.").append(val.getColumn()).append(";");
                break;
            case ORR:
                sb.append("ctx._source.").append(val.getColumn()).append(" |= params.").append(val.getColumn()).append(";");
                break;
        }
    }

    public static class UpdateScript extends BaseBean {

        public String source;

        public Map<String, Serializable> params;
    }
}
