/*
 *
 */
package org.redkalex.source.pgsql;

import java.io.Serializable;
import java.nio.ByteBuffer;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public interface PgColumnDecodeable {

    Serializable decode(ByteBuffer buffer, ByteArray array, Attribute attr, int len);
}
