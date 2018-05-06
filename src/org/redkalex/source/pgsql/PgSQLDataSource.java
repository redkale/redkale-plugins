/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.io.Serializable;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.sql.*;
import java.time.format.*;
import static java.time.format.DateTimeFormatter.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import org.redkale.net.AsyncConnection;
import org.redkale.service.Local;
import org.redkale.source.*;
import org.redkale.util.*;
import static org.redkalex.source.pgsql.PgPoolSource.CONN_ATTR_BYTESBAME;

/**
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
@SuppressWarnings("unchecked")
@ResourceType(DataSource.class)
public class PgSQLDataSource extends DataSqlSource<AsyncConnection> {

    private static final byte[] TRUE = new byte[]{'t'};

    private static final byte[] FALSE = new byte[]{'f'};

    private static final DateTimeFormatter TIMESTAMP_FORMAT = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(ISO_LOCAL_DATE)
        .appendLiteral(' ')
        .append(ISO_LOCAL_TIME)
        .toFormatter();

    private static final byte[] DESCRIBE_EXECUTE_CLOSE_SYNC_BYTES;

    static {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        { // DESCRIBE
            buffer.put((byte) 'D');
            buffer.putInt(4 + 1 + 1);
            buffer.put((byte) 'S');
            buffer.put((byte) 0);
        }
        { // EXECUTE
            buffer.put((byte) 'E');
            buffer.putInt(4 + 1 + 4);
            buffer.put((byte) 0);
            buffer.putInt(0);
        }
        { // CLOSE
            buffer.put((byte) 'C');
            buffer.putInt(4 + 1 + 1);
            buffer.put((byte) 'S');
            buffer.put((byte) 0);
        }
        { // SYNC
            buffer.put((byte) 'S');
            buffer.putInt(4);
        }
        buffer.flip();
        byte[] bs = new byte[buffer.remaining()];
        buffer.get(bs);
        DESCRIBE_EXECUTE_CLOSE_SYNC_BYTES = bs;
    }

    public PgSQLDataSource(String unitName, URL persistxml, Properties readprop, Properties writeprop) {
        super(unitName, persistxml, readprop, writeprop);
    }

    @Local
    protected PoolSource<AsyncConnection> readPoolSource() {
        return readPool;
    }

    @Local
    protected PoolSource<AsyncConnection> writePoolSource() {
        return writePool;
    }

    protected static String getCString(ByteBuffer buffer, byte[] store) {
        int i = 0;
        for (byte c = buffer.get(); c != 0; c = buffer.get()) {
            store[i++] = c;
        }
        return new String(store, 0, i, StandardCharsets.UTF_8);
    }

    protected static ByteBuffer putCString(ByteBuffer buffer, String string) {
        buffer.put(string.getBytes(StandardCharsets.UTF_8));
        buffer.put((byte) 0);
        return buffer;
    }

    protected static ByteBufferWriter putCString(ByteBufferWriter buffer, String string) {
        buffer.put(string.getBytes(StandardCharsets.UTF_8));
        buffer.put((byte) 0);
        return buffer;
    }

    @Override
    protected String prepareParamSign(int index) {
        return "$" + index;
    }

    @Override
    protected final boolean isAsync() {
        return true;
    }

    @Override
    protected PoolSource<AsyncConnection> createPoolSource(DataSource source, String rwtype, Properties prop) {
        return new PgPoolSource(rwtype, prop, logger, bufferPool, executor);
    }

    @Override
    protected <T> CompletableFuture<Integer> insertDB(EntityInfo<T> info, T... values) {
        final Attribute<T, Serializable>[] attrs = info.getInsertAttributes();
        final Object[][] objs = new Object[values.length][];
        for (int i = 0; i < values.length; i++) {
            final Object[] params = new Object[attrs.length];
            for (int j = 0; j < attrs.length; j++) {
                params[j] = attrs[j].get(values[i]);
            }
            objs[i] = params;
        }
        return writePool.pollAsync().thenCompose((conn) -> executeUpdate(conn, info.getInsertDollarPrepareSQL(values[0]), values, true, objs));
    }

    @Override
    protected <T> CompletableFuture<Integer> deleteDB(EntityInfo<T> info, Flipper flipper, String sql0) {
        final String sql = flipper == null || flipper.getLimit() <= 0 ? sql0 : (sql0 + " LIMIT " + flipper.getLimit());
        if (info.isLoggable(logger, Level.FINEST)) logger.finest(info.getType().getSimpleName() + " delete sql=" + sql);
        return writePool.pollAsync().thenCompose((conn) -> executeUpdate(conn, sql, null, false));
    }

    @Override
    protected <T> CompletableFuture<Integer> updateDB(EntityInfo<T> info, final T... values) {
        final Attribute<T, Serializable> primary = info.getPrimary();
        final Attribute<T, Serializable>[] attrs = info.getUpdateAttributes();
        final Object[][] objs = new Object[values.length][];
        for (int i = 0; i < values.length; i++) {
            final Object[] params = new Object[attrs.length + 1];
            for (int j = 0; j < attrs.length; j++) {
                params[j] = attrs[j].get(values[i]);
            }
            params[attrs.length] = primary.get(values[i]); //最后一个是主键
            objs[i] = params;
        }
        return writePool.pollAsync().thenCompose((conn) -> executeUpdate(conn, info.getUpdateDollarPrepareSQL(values[0]), null, false, objs));
    }

    @Override
    protected <T> CompletableFuture<Integer> updateDB(EntityInfo<T> info, Flipper flipper, String sql0, boolean prepared, Object... params) {
        final String sql = flipper == null || flipper.getLimit() <= 0 ? sql0 : (sql0 + " LIMIT " + flipper.getLimit());
        if (info.isLoggable(logger, Level.FINEST)) logger.finest(info.getType().getSimpleName() + " update sql=" + sql);
        Object[][] objs = params == null || params.length == 0 ? null : new Object[][]{params};
        return writePool.pollAsync().thenCompose((conn) -> executeUpdate(conn, sql, null, false, objs));
    }

    @Override
    protected <T, N extends Number> CompletableFuture<Map<String, N>> getNumberMapDB(EntityInfo<T> info, String sql, FilterFuncColumn... columns) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected <T> CompletableFuture<Number> getNumberResultDB(EntityInfo<T> info, String sql, Number defVal, String column) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected <T, K extends Serializable, N extends Number> CompletableFuture<Map<K, N>> queryColumnMapDB(EntityInfo<T> info, String sql, String keyColumn) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected <T> CompletableFuture<T> findDB(EntityInfo<T> info, String sql, boolean onlypk, SelectColumn selects) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected <T> CompletableFuture<Serializable> findColumnDB(EntityInfo<T> info, String sql, boolean onlypk, String column, Serializable defValue) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected <T> CompletableFuture<Boolean> existsDB(EntityInfo<T> info, String sql, boolean onlypk) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected <T> CompletableFuture<Sheet<T>> querySheetDB(EntityInfo<T> info, boolean needtotal, SelectColumn selects, Flipper flipper, FilterNode node) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private static byte[] formatPrepareParam(Object param) {
        if (param == null) return null;
        if (param instanceof byte[]) return (byte[]) param;
        if (param instanceof Boolean) return (Boolean) param ? TRUE : FALSE;
        if (param instanceof java.sql.Date) return ISO_LOCAL_DATE.format(((java.sql.Date) param).toLocalDate()).getBytes(UTF_8);
        if (param instanceof java.sql.Time) return ISO_LOCAL_TIME.format(((java.sql.Time) param).toLocalTime()).getBytes(UTF_8);
        if (param instanceof java.sql.Timestamp) return TIMESTAMP_FORMAT.format(((java.sql.Timestamp) param).toLocalDateTime()).getBytes(UTF_8);
        return String.valueOf(param).getBytes(UTF_8);
    }

    protected <T> CompletableFuture<Integer> executeUpdate(final AsyncConnection conn, final String sql, final T[] values, final boolean insert, final Object[]... parameters) {
        final byte[] bytes = conn.getAttribute(CONN_ATTR_BYTESBAME);
        final ByteBufferWriter writer = ByteBufferWriter.create(bufferPool);
        {
            writer.put((byte) 'P');
            int start = writer.position();
            writer.putInt(0);
            writer.put((byte) 0); // unnamed prepared statement
            putCString(writer, sql);
            writer.putShort((short) 0); // no parameter types
            writer.putInt(start, writer.position() - start);
        }
        { // DESCRIBE
            writer.put((byte) 'D');
            writer.putInt(4 + 1 + 1);
            writer.put((byte) 'S');
            writer.put((byte) 0);
        }
        if (parameters != null && parameters.length > 0) {
            for (Object[] params : parameters) {
                { // BIND
                    writer.put((byte) 'B');
                    int start = writer.position();
                    writer.putInt(0);
                    writer.put((byte) 0); // portal
                    writer.put((byte) 0); // prepared statement
                    writer.putShort((short) 0); // number of format codes
                    if (params == null || params.length == 0) {
                        writer.putShort((short) 0); // number of parameters
                    } else {
                        writer.putShort((short) params.length); // number of parameters
                        for (Object param : params) {
                            byte[] bs = formatPrepareParam(param);
                            if (bs == null) {
                                writer.putInt(-1);
                            } else {
                                writer.putInt(bs.length);
                                writer.put(bs);
                            }
                        }
                    }
                    writer.putShort((short) 0);
                    writer.putInt(start, writer.position() - start);
                }
                { // EXECUTE
                    writer.put((byte) 'E');
                    writer.putInt(4 + 1 + 4);
                    writer.put((byte) 0);
                    writer.putInt(0);
                }
            }
        } else {
            { // BIND
                writer.put((byte) 'B');
                int start = writer.position();
                writer.putInt(0);
                writer.put((byte) 0); // portal  
                writer.put((byte) 0); // prepared statement  
                writer.putShort((short) 0); // 后面跟着的参数格式代码的数目(在下面的 C 中说明)。这个数值可以是零，表示没有参数，或者是参数都使用缺省格式(文本)
                writer.putShort((short) 0);  //number of format codes 参数格式代码。目前每个都必须是零(文本)或者一(二进制)。
                writer.putShort((short) 0);// number of parameters 后面跟着的参数值的数目(可能为零)。这些必须和查询需要的参数个数匹配。
                writer.putInt(start, writer.position() - start);
            }
            { // EXECUTE
                writer.put((byte) 'E');
                writer.putInt(4 + 1 + 4);
                writer.put((byte) 0); //portal 要执行的入口的名字(空字符串选定未命名的入口)。
                writer.putInt(0); //要返回的最大行数，如果入口包含返回行的查询(否则忽略)。零标识"没有限制"。
            }
        }
        { // SYNC
            writer.put((byte) 'S');
            writer.putInt(4);
        }
        final ByteBuffer[] buffers = writer.toBuffers();
        final CompletableFuture<Integer> future = new CompletableFuture();
        conn.write(buffers, buffers, new CompletionHandler<Integer, ByteBuffer[]>() {
            @Override
            public void completed(Integer result, ByteBuffer[] attachment1) {
                if (result < 0) {
                    failed(new SQLException("Write Buffer Error"), attachment1);
                    return;
                }
                int index = -1;
                for (int i = 0; i < attachment1.length; i++) {
                    if (attachment1[i].hasRemaining()) {
                        index = i;
                        break;
                    }
                    bufferPool.accept(attachment1[i]);
                }
                if (index == 0) {
                    conn.write(attachment1, attachment1, this);
                    return;
                } else if (index > 0) {
                    ByteBuffer[] newattachs = new ByteBuffer[attachment1.length - index];
                    System.arraycopy(attachment1, index, newattachs, 0, newattachs.length);
                    conn.write(newattachs, newattachs, this);
                    return;
                }

                final ByteBuffer buffer = bufferPool.get();
                conn.read(buffer, null, new CompletionHandler<Integer, Void>() {
                    @Override
                    public void completed(Integer result, Void attachment2) {
                        if (result < 0) {
                            failed(new SQLException("Read Buffer Error"), attachment2);
                            return;
                        }
                        buffer.flip();
                        boolean endok = false;
                        boolean futureover = false;
                        while (buffer.hasRemaining()) {
                            final char cmd = (char) buffer.get();
                            int length = buffer.getInt();
                            System.out.println("---------------cmd:" + cmd + "--------length:" + length);
                            switch (cmd) {
                                case 'E':
                                    byte[] field = new byte[255];
                                    String level = null,
                                     code = null,
                                     message = null;
                                    for (byte type = buffer.get(); type != 0; type = buffer.get()) {
                                        String value = getCString(buffer, field);
                                        if (type == (byte) 'S') {
                                            level = value;
                                        } else if (type == 'C') {
                                            code = value;
                                        } else if (type == 'M') {
                                            message = value;
                                        }
                                    }
                                    future.completeExceptionally(new SQLException(message, code, 0));
                                    futureover = true;
                                    break;
                                case 'C':
                                    String val = getCString(buffer, bytes);
                                    int pos = val.lastIndexOf(' ');
                                    if (pos > 0) {
                                        future.complete(Integer.parseInt(val.substring(pos + 1)));
                                        futureover = true;
                                    }
                                    break;
                                case 'Z':
                                    buffer.position(buffer.position() + length - 4);
                                    endok = true;
                                    break;
                                default:
                                    buffer.position(buffer.position() + length - 4);
                                    break;
                            }
                        }
                        bufferPool.accept(buffer);
                        if (!futureover) future.completeExceptionally(new SQLException("SQL(" + sql + ") executeUpdate error"));
                        if (endok) {
                            writePool.offerConnection(conn);
                        } else {
                            conn.dispose();
                        }
                    }

                    @Override
                    public void failed(Throwable exc, Void attachment2) {
                        bufferPool.accept(buffer);
                        future.completeExceptionally(exc);
                        conn.dispose();
                    }
                });
            }

            @Override
            public void failed(Throwable exc, ByteBuffer[] attachment1) {
                for (int i = 0; i < attachment1.length; i++) {
                    bufferPool.accept(attachment1[i]);
                }
                exc.printStackTrace();
            }
        });
        return future;
    }
}
