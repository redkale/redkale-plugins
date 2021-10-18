/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.redkale.net.*;
import org.redkale.net.client.*;
import org.redkale.source.EntityInfo;

/**
 *
 * @author zhangjx
 * @param <R> ClientRequest
 */
public class MyClientConnection<R extends MyClientRequest> extends ClientConnection<R, MyResultSet> {

    private Map<String, Long> cacheExtendedIndexs = new HashMap<>();

    private Map<String, AtomicBoolean> cacheExtendedPrepares = new HashMap<>();

    private Map<String, MyPrepareDesc> cacheExtendedDescs = new HashMap<>();

    int clientCapabilitiesFlag;

    public MyClientConnection(MyClient client, int index, AsyncConnection channel) {
        super(client, index, channel);
    }

    @Override
    protected void preComplete(MyResultSet resp, R req, Throwable exc) {
        if (resp != null) resp.request = req;
    }

    @Override
    protected void pauseWriting(boolean flag) {
        super.pauseWriting(flag);
    }

    public AtomicBoolean getPrepareFlag(String prepareSql) {
        return cacheExtendedPrepares.computeIfAbsent(prepareSql, t -> new AtomicBoolean());
    }

    public Long getStatementIndex(String prepareSql) {
        return cacheExtendedIndexs.get(prepareSql);
    }

    public MyPrepareDesc getPrepareDesc(String prepareSql) {
        return cacheExtendedDescs.get(prepareSql);
    }

    public void putStatementIndex(String prepareSql, long id) {
        cacheExtendedIndexs.put(prepareSql, id);
    }

    public void putPrepareDesc(String prepareSql, MyPrepareDesc desc) {
        cacheExtendedDescs.put(prepareSql, desc);
    }

    public MyResultSet pollResultSet() {
        return new MyResultSet();
    }

    public MyReqInsert pollReqInsert(WorkThread workThread, EntityInfo info) {
        MyReqInsert rs = new MyReqInsert();
        rs.info = info;
        rs.currThread(workThread);
        return rs;
    }

    public MyReqUpdate pollReqUpdate(WorkThread workThread, EntityInfo info) {
        MyReqUpdate rs = new MyReqUpdate();
        rs.info = info;
        rs.currThread(workThread);
        return rs;
    }

    public MyReqQuery pollReqQuery(WorkThread workThread, EntityInfo info) {
        MyReqQuery rs = new MyReqQuery();
        rs.info = info;
        rs.currThread(workThread);
        return rs;
    }

    public MyReqExtended pollReqExtended(WorkThread workThread, EntityInfo info) {
        MyReqExtended rs = new MyReqExtended();
        rs.info = info;
        rs.currThread(workThread);
        return rs;
    }
}
