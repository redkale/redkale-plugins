/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import org.redkale.net.client.*;

/**
 *
 * @author zhangjx
 * @param <T> 泛型
 */
public class PgClientFuture<T> extends ClientFuture<T> {

    protected Runnable callback;

    public PgClientFuture() {
        super();
    }

    public PgClientFuture(ClientRequest request) {
        super(request);
    }

    @Override
    public <U> PgClientFuture<U> newIncompleteFuture() {
        return new PgClientFuture<>();
    }

    @Override
    public boolean complete(T value) {
        if (callback != null) {
            callback.run();
            callback = null;
        }
        return super.complete(value);
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        if (callback != null) {
            callback.run();
            callback = null;
        }
        return super.completeExceptionally(ex);
    }
}
