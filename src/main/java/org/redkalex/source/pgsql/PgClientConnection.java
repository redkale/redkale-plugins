/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.redkale.net.*;
import org.redkale.net.client.*;
import org.redkale.source.EntityInfo;

/**
 *
 * @author zhangjx
 */
public class PgClientConnection extends ClientConnection<PgClientRequest, PgResultSet> {

    private final Map<String, Long> cacheExtendedIndexs = new HashMap<>();

    private final Map<String, AtomicBoolean> cacheExtendedPrepares = new HashMap<>();

    private final Map<String, PgRowDesc> cacheExtendedDescs = new HashMap<>();

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

    @Override
    protected void preComplete(PgResultSet resp, PgClientRequest req, Throwable exc) {
        if (resp != null) resp.request = req;
    }

    public AtomicBoolean getPrepareFlag(String prepareSql) {
        return cacheExtendedPrepares.computeIfAbsent(prepareSql, t -> new AtomicBoolean());
    }

    public Long getStatementIndex(String prepareSql) {
        return cacheExtendedIndexs.get(prepareSql);
    }

    public Long createStatementIndex(String prepareSql) {
        long rs = ((PgClient) client).extendedStatementid(prepareSql);
        cacheExtendedIndexs.put(prepareSql, rs);
        return rs;
    }

    public PgRowDesc getPrepareDesc(String prepareSql) {
        return cacheExtendedDescs.get(prepareSql);
    }

    public void putStatementIndex(String prepareSql, long id) {
        cacheExtendedIndexs.put(prepareSql, id);
    }

    public void putPrepareDesc(String prepareSql, PgRowDesc desc) {
        cacheExtendedDescs.put(prepareSql, desc);
    }

    public PgResultSet pollResultSet(EntityInfo info) {
        PgResultSet rs = new PgResultSet();
        rs.info = info;
        return rs;
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
        PgReqExtended rs = new PgReqExtended();
        rs.info = info;
        rs.currThread(workThread);
        return rs;
    }

    public int getIndex() {
        return index;
    }
}
