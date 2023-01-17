/*
 *
 */
package org.redkalex.source.pgsql;

import java.io.Serializable;
import org.redkale.util.ByteArray;

/**
 *
 * @author zhangjx
 */
public interface PgColumnEncodeable {

    public void encode(ByteArray array, Serializable value);
}
