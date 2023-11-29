/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import org.redkale.net.*;
import org.redkale.net.client.*;
import org.redkale.source.EntityInfo;

/**
 *
 * @author zhangjx
 */
public class MyClientConnection extends ClientConnection<MyClientRequest, MyResultSet> {

    private final Map<String, Long> cacheExtendedIndexs = new HashMap<>();

    private final Map<String, AtomicBoolean> cacheExtendedPrepares = new HashMap<>();

    private final Map<String, MyPrepareDesc> cacheExtendedDescs = new HashMap<>();

    MyRespHandshakeResultSet handshake;

    //int clientCapabilitiesFlag;
    //
    public MyClientConnection(MyClient client, AsyncConnection channel) {
        super(client, channel);
    }

    @Override
    protected ClientCodec createCodec() {
        return new MyClientCodec(this);
    }

    @Override
    protected void preComplete(MyResultSet resp, MyClientRequest req, Throwable exc) {
        if (resp != null) {
            resp.request = req;
        }
    }

    protected boolean autoddl() {
        return ((MyClient) client).autoddl;
    }

    protected Logger logger() {
        return ((MyClient) client).logger();
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

    public MyResultSet pollResultSet(EntityInfo info) {
        MyResultSet rs = new MyResultSet();
        rs.info = info;
        return rs;
    }

    public MyReqUpdate pollReqUpdate(WorkThread workThread, EntityInfo info) {
        MyReqUpdate rs = new MyReqUpdate();
        rs.info = info;
        rs.workThread(workThread);
        return rs;
    }

    public MyReqQuery pollReqQuery(WorkThread workThread, EntityInfo info) {
        MyReqQuery rs = new MyReqQuery();
        rs.info = info;
        rs.workThread(workThread);
        return rs;
    }

    public MyReqExtended pollReqExtended(WorkThread workThread, EntityInfo info) {
        MyReqExtended rs = new MyReqExtended();
        rs.info = info;
        rs.workThread(workThread);
        return rs;
    }
}
