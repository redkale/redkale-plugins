/*
 *
 */
package org.redkalex.source.pgsql;

import java.io.Serializable;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public interface PgColumnEncodeable {

    //attr可能会为null
    public void encode(ByteArray array, Attribute attr, Serializable value);
}
