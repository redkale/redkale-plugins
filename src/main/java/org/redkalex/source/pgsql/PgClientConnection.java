/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.util.*;
import org.redkale.net.*;
import org.redkale.net.client.*;
import org.redkale.source.EntityInfo.EntityColumn;
import org.redkale.source.*;
import org.redkale.util.*;
import org.redkalex.source.pgsql.PgPrepareDesc.PgExtendMode;

/**
 *
 * @author zhangjx
 */
public class PgClientConnection extends ClientConnection<PgClientRequest, PgResultSet> {

    private final Map<String, PgPrepareDesc> cachePreparedDescs = new HashMap<>();

    private final ObjectPool<PgReqExtended> reqExtendedPool = ObjectPool.createUnsafePool(Thread.currentThread(), 256, ObjectPool.createSafePool(256, t -> new PgReqExtended(), PgReqExtended::prepare, PgReqExtended::recycle));

    public PgClientConnection(PgClient client, int index, AsyncConnection channel) {
        super(client, index, channel);
    }

    @Override
    protected ClientCodec createCodec() {
        return new PgClientCodec(this);
    }

    protected boolean autoddl() {
        return ((PgClient) client).autoddl;
    }

    protected void offerResultSet(PgReqExtended req, PgResultSet rs) {
        PgClientCodec c = getCodec();
        c.offerResultSet(rs);
        reqExtendedPool.accept(req);
    }

    @Override
    protected void preComplete(PgResultSet resp, PgClientRequest req, Throwable exc) {
        if (resp != null) {
            resp.request = req;
        }
    }

    public PgPrepareDesc getPgPrepareDesc(String prepareSql) {
        return cachePreparedDescs.get(prepareSql);
    }

    public PgPrepareDesc createPgPrepareDesc(int type, PgExtendMode mode, EntityInfo info, String sql) {
        Attribute[] paramAttrs;
        EntityColumn[] paramCols;
        Attribute[] resultAttrs;
        EntityColumn[] resultCols;
        if (mode == PgExtendMode.INSERT_ENTITY) {
            paramAttrs = info.getInsertAttributes();
            paramCols = info.getInsertColumns();
            resultAttrs = new Attribute[0];
            resultCols = new EntityColumn[0];
        } else if (mode == PgExtendMode.FIND_ENTITY) {
            paramAttrs = info.getPrimaryOneArray();
            paramCols = info.getPrimaryColumnOneArray();
            resultAttrs = info.getQueryAttributes();
            resultCols = info.getQueryColumns();
        } else if (mode == PgExtendMode.FINDS_ENTITY) {
            paramAttrs = info.getPrimaryOneArray();
            paramCols = info.getPrimaryColumnOneArray();
            resultAttrs = info.getQueryAttributes();
            resultCols = info.getQueryColumns();
        } else if (mode == PgExtendMode.UPDATE_ENTITY) {
            paramAttrs = info.getUpdateEntityAttributes();
            paramCols = info.getUpdateEntityColumns();
            resultAttrs = new Attribute[0];
            resultCols = new EntityColumn[0];
        } else if (mode == PgExtendMode.LISTALL_ENTITY) {
            paramAttrs = new Attribute[0];
            paramCols = new EntityColumn[0];
            resultAttrs = info.getQueryAttributes();
            resultCols = info.getQueryColumns();
        } else if (mode == PgExtendMode.UPCASE_ENTITY) {
            String in = sql.substring(sql.lastIndexOf('(') + 1, sql.lastIndexOf(')'));
            int size = (int) in.chars().filter(c -> c == '$').count();
            paramAttrs = new Attribute[size * 2];
            paramCols = new EntityColumn[size * 2];
            for (int i = 0; i < size; i++) {
                paramAttrs[i] = info.getPrimary();
                paramCols[i] = info.getPrimaryColumn();
                Attribute[] attrs = info.getUpdateEntityAttributes();
                EntityColumn[] cols = info.getUpdateEntityColumns();
                for (int j = 0; j < attrs.length; j++) {
                    paramAttrs[size + j] = attrs[j];
                    paramCols[size + j] = cols[j];
                }
            }
            resultAttrs = new Attribute[0];
            resultCols = new EntityColumn[0];
        } else {
            throw new SourceException("PgExtendMode (" + mode + ") is illegal");
        }
        PgPrepareDesc prepareDesc = new PgPrepareDesc(type, mode, sql, nextSequence(), paramAttrs, paramCols, resultAttrs, resultCols);
        cachePreparedDescs.put(sql, prepareDesc);
        return prepareDesc;
    }

    public PgReqInsert pollReqInsert(WorkThread workThread, EntityInfo info) {
        PgReqInsert rs = new PgReqInsert();
        rs.info = info;
        rs.currThread(workThread);
        return rs;
    }

    public PgReqUpdate pollReqUpdate(WorkThread workThread, EntityInfo info) {
        PgReqUpdate rs = new PgReqUpdate();
        rs.info = info;
        rs.currThread(workThread);
        return rs;
    }

    public PgReqQuery pollReqQuery(WorkThread workThread, EntityInfo info) {
        PgReqQuery rs = new PgReqQuery();
        rs.info = info;
        rs.currThread(workThread);
        return rs;
    }

    public PgReqExtended pollReqExtended(WorkThread workThread, EntityInfo info) {
        PgReqExtended rs = reqExtendedPool.get();
        rs.info = info;
        rs.currThread(workThread);
        return rs;
    }

    public int getIndex() {
        return index;
    }

    private long sequence;

    private byte[] nextSequence() {
        int len = 3 // 3 leading zeroes
            + (64 - Long.numberOfLeadingZeros(sequence) + 3) / 4 // hex characters
            + 1;  // tailing null byte
        len = Math.max(8, len);  // at least 7 hex digits plus null byte
        byte[] hex = new byte[len];
        int pos = len - 1;
        hex[pos--] = '\0';
        long n = sequence;
        while (n != 0) {
            long c = n & 0xf;
            hex[pos--] = c < 10 ? (byte) ('0' + c) : (byte) ('A' + c - 10);
            n >>>= 4;
        }
        while (pos >= 0) {
            hex[pos--] = '0';
        }
        sequence++;
        return hex;
    }
}
