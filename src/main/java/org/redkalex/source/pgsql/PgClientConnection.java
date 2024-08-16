/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import org.redkale.net.*;
import org.redkale.net.client.*;
import org.redkale.source.*;
import org.redkale.util.*;
import org.redkalex.source.pgsql.PgPrepareDesc.PgExtendMode;

/** @author zhangjx */
public class PgClientConnection extends ClientConnection<PgClientRequest, PgResultSet> {

    private PgPrepareDesc lastPrepareDesc;

    private final Map<String, PgPrepareDesc> cachePreparedDescs = new HashMap<>();

    private final LinkedBlockingQueue<ClientFuture> writeQueue = new LinkedBlockingQueue();

    private final Thread writeThread;

    public PgClientConnection(PgClient client, AsyncConnection channel) {
        super(client, channel);
        this.writeThread = new Thread(this::runInWriteThread);
        this.writeThread.setDaemon(true);
        this.writeThread.start();
    }

    @Override
    protected ClientCodec createCodec() {
        return new PgClientCodec(this);
    }

    protected boolean autoddl() {
        return ((PgClient) client).autoddl;
    }

    @Override // AsyncConnection.beforeCloseListener
    public void accept(AsyncConnection t) {
        super.accept(t);
        this.writeQueue.add(ClientFuture.NIL);
        this.writeThread.interrupt();
    }

    private void runInWriteThread() {
        final List<ClientFuture> list = new ArrayList<>();
        while (true) {
            try {
                ClientFuture respFuture = writeQueue.take();
                if (respFuture == ClientFuture.NIL) {
                    return;
                }
                boolean over = false;
                list.clear();
                list.add(respFuture);
                while ((respFuture = writeQueue.poll()) != null) {
                    if (respFuture == ClientFuture.NIL) {
                        over = true;
                        break;
                    } else {
                        list.add(respFuture);
                    }
                }
                super.sendRequestToChannel(list.toArray(new ClientFuture[list.size()]));
                list.clear();
                if (over) {
                    return;
                }
            } catch (InterruptedException ie) {
                break;
            } catch (Throwable e) {
                // do nothing
            }
        }
    }

    @Override
    protected void offerRespFuture(ClientFuture respFuture) {
        super.offerRespFuture(respFuture);
        writeQueue.offer(respFuture);
    }

    @Override
    protected void sendRequestInLocking(ClientFuture... respFutures) {
        // do nothing
    }

    protected void offerResultSet(PgReqExtended req, PgResultSet rs) {
        PgClientCodec c = getCodec();
        c.offerResultSet(rs);
    }

    public PgPrepareDesc getPgPrepareDesc(String prepareSql) {
        PgPrepareDesc desc = lastPrepareDesc;
        if (desc != null && desc.sql().equals(prepareSql)) {
            return desc;
        }
        desc = cachePreparedDescs.get(prepareSql);
        lastPrepareDesc = desc;
        return desc;
    }

    public PgPrepareDesc createPgPrepareDesc(int type, PgExtendMode mode, EntityInfo info, String sql, int paramLen) {
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
            Attribute[] attrs = info.getUpdateAttributes();
            paramAttrs = new Attribute[size * (attrs.length + 1)];
            paramCols = new EntityColumn[paramAttrs.length];
            for (int i = 0; i < size; i++) {
                paramAttrs[i] = info.getPrimary();
                paramCols[i] = info.getPrimaryColumn();
            }
            EntityColumn[] cols = info.getUpdateColumns();
            for (int j = 1; j <= attrs.length; j++) {
                for (int i = 0; i < size; i++) {
                    paramAttrs[size * j + i] = attrs[j - 1];
                    paramCols[size * j + i] = cols[j - 1];
                }
            }
            resultAttrs = new Attribute[0];
            resultCols = new EntityColumn[0];
        } else if (mode == PgExtendMode.OTHER_NATIVE) {
            paramAttrs = new Attribute[paramLen];
            paramCols = new EntityColumn[paramLen];
            resultAttrs = new Attribute[0];
            resultCols = new EntityColumn[0];
        } else {
            throw new SourceException("PgExtendMode (" + mode + ") is illegal");
        }
        PgPrepareDesc prepareDesc =
                new PgPrepareDesc(type, mode, sql, nextSequence(), paramAttrs, paramCols, resultAttrs, resultCols);
        cachePreparedDescs.put(sql, prepareDesc);
        return prepareDesc;
    }

    public PgReqInsert pollReqInsert(WorkThread workThread, EntityInfo info) {
        PgReqInsert rs = new PgReqInsert();
        rs.prepare();
        rs.info = info;
        rs.workThread(workThread);
        return rs;
    }

    public PgReqUpdate pollReqUpdate(WorkThread workThread, EntityInfo info) {
        PgReqUpdate rs = new PgReqUpdate();
        rs.prepare();
        rs.info = info;
        rs.workThread(workThread);
        return rs;
    }

    public PgReqQuery pollReqQuery(WorkThread workThread, EntityInfo info) {
        PgReqQuery rs = new PgReqQuery();
        rs.prepare();
        rs.info = info;
        rs.workThread(workThread);
        return rs;
    }

    public PgReqExtended pollReqExtended(WorkThread workThread, EntityInfo info) {
        PgReqExtended rs = new PgReqExtended();
        rs.prepare();
        rs.info = info;
        rs.workThread(workThread);
        return rs;
    }

    private long sequence;

    private byte[] nextSequence() {
        int len = 3 // 3 leading zeroes
                + (64 - Long.numberOfLeadingZeros(sequence) + 3) / 4 // hex characters
                + 1; // tailing null byte
        len = Math.max(8, len); // at least 7 hex digits plus null byte
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
