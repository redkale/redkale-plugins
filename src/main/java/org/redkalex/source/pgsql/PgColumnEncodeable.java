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

    public void encode(ByteArray array, Attribute attr, Serializable value);
}
