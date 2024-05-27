/*
 *
 */
package org.redkalex.source.pgsql;

import java.io.Serializable;
import java.nio.ByteBuffer;
import org.redkale.annotation.Nullable;
import org.redkale.util.*;

/** @author zhangjx */
public interface PgColumnDecodeable {

	// attr可能会为null
	Serializable decode(ByteBuffer buffer, ByteArray array, @Nullable Attribute attr, int len);
}
