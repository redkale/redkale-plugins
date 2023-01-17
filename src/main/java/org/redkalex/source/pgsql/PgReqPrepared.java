/*
 *
 */
package org.redkalex.source.pgsql;

import java.util.Objects;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.ByteArray;
import static org.redkalex.source.pgsql.PgClientRequest.writeUTF8String;

/**
 *
 * @author zhangjx
 */
public class PgReqPrepared extends PgClientRequest {

    protected final PgPrepareDesc prepareDesc;

    public <T> PgReqPrepared(final PgPrepareDesc prepareDesc) {
        this.prepareDesc = prepareDesc;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "_" + Objects.hashCode(this) + "{sql = '" + prepareDesc.sql() + "', prepareDesc = " + prepareDesc + "}";
    }

    @Override
    public int getType() {
        return prepareDesc.type();
    }

    private void writeParse(ByteArray array) { // PARSE
        array.putByte('P');
        int start = array.length();
        array.putInt(0); //命令块长度
        array.put(prepareDesc.statement());
        writeUTF8String(array, prepareDesc.sql());
        PgColumnFormat[] formats = prepareDesc.paramFormats();
        if (formats == null || formats.length == 0) {
            array.putShort(0); // no parameter types
        } else {
            array.putShort(formats.length);
            for (PgColumnFormat f : formats) {
                array.putInt(f.oid());
            }
        }
        array.putInt(start, array.length() - start);
    }

    private void writeDescribe(ByteArray array) { // DESCRIBE
        array.putByte('D');
        array.putInt(4 + 1 + prepareDesc.statement().length);
        array.putByte('S');
        array.put(prepareDesc.statement());
    }

    @Override
    public void writeTo(ClientConnection conn, ByteArray array) {
        writeParse(array);
        writeDescribe(array);
        writeSync(array);
    }

    public static enum PgReqExtendMode {
        FIND, FINDS, LIST_ALL, OTHER;
    }
}
