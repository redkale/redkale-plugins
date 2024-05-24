/*
 *
 */
package org.redkalex.source.parser;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;
import java.util.logging.*;
import org.redkale.annotation.ResourceType;
import org.redkale.source.DataNativeSqlInfo;
import org.redkale.source.DataNativeSqlParser;
import org.redkale.source.DataNativeSqlStatement;
import org.redkale.util.ObjectRef;

/**
 * 基于jsqlparser的DataNativeSqlParser实现类
 *
 *
 * @author zhangjx
 */
@ResourceType(DataNativeSqlParser.class)
public class DataNativeJsqlParser implements DataNativeSqlParser {

    protected final Logger logger = Logger.getLogger(DataNativeJsqlParser.class.getSimpleName());

    private final ConcurrentHashMap<String, NativeParserInfo> parserInfo = new ConcurrentHashMap();

    @Override
    public DataNativeSqlInfo parse(IntFunction<String> signFunc, String dbType, String rawSql) {
        return parserInfo.computeIfAbsent(rawSql, sql -> new NativeParserInfo(sql));
    }

    @Override
    public DataNativeSqlStatement parse(IntFunction<String> signFunc, String dbType, String rawSql, boolean countable, Map<String, Object> params) {
        NativeParserInfo info = parserInfo.computeIfAbsent(rawSql, sql -> new NativeParserInfo(sql));
        ObjectRef<String> templetRef = new ObjectRef<>();
        NativeSqlTemplet templet = info.createTemplet(params);
        if (logger.isLoggable(Level.FINER)) {
            logger.log(Level.FINER, DataNativeSqlParser.class.getSimpleName() + " parse. rawSql: " + rawSql
                + ", dynamic: " + info.isDynamic() + ", templetSql: " + templetRef.get());
        }
        NativeParserNode node = info.loadParserNode(signFunc, dbType, templetRef.get(), countable);
        DataNativeSqlStatement statement = node.loadStatement(signFunc, templet.getTempletParams());
        if (logger.isLoggable(Level.FINE)) {
            String countSql = countable ? (", nativeCountSql: " + statement.getNativeCountSql()) : "";
            logger.log(Level.FINE, DataNativeSqlParser.class.getSimpleName() + " parse. rawSql: " + rawSql + ", nativeSql: " + statement.getNativeSql()
                + countSql + ", paramNames: " + statement.getParamNames());
        }
        return statement;
    }

}
