/*
 *
 */

package org.redkalex.yaml;

import java.util.Collection;
import java.util.Map;
import org.redkale.convert.json.JsonConvert;
import org.redkale.util.AnyValue;
import org.redkale.util.AnyValueWriter;
import org.redkale.util.YamlProvider;
import org.yaml.snakeyaml.Yaml;

/**
 *
 * @author zhangjx
 */
public class SnakeYamlLoader implements YamlProvider.YamlLoader {

    /**
     * 将yml内容转换成AnyValue
     *
     * @param content yml内容
     * @return  AnyValue
     */
    @Override
    public AnyValue read(String content) {
        Map<String, Object> map = new Yaml().load(content);
        AnyValueWriter writer = AnyValue.create();
        read(writer, map);
        return writer;
    }

    protected static void read(AnyValueWriter writer, Map<String, Object> map) {
        map.forEach((k, v) -> {
            if (v == null) {
                writer.addValue(k, (String) null);
            } else if (v instanceof Map) {
                AnyValueWriter item = AnyValue.create();
                writer.addValue(k, item);
                read(item, (Map) v);
            } else if (v instanceof Collection) {
                read(writer, k, (Collection) v);
            } else {
                writer.addValue(k, JsonConvert.root().convertTo(v));
            }
        });
    }

    protected static void read(AnyValueWriter writer, String k, Collection list) {
        for (Object v : list) {
            if (v == null) {
                writer.addValue(k, (String) null);
            } else if (v instanceof Map) {
                AnyValueWriter item = AnyValue.create();
                writer.addValue(k, item);
                read(item, (Map) v);
            } else if (v instanceof Collection) {
                read(writer, k, (Collection) v);
            } else {
                writer.addValue(k, JsonConvert.root().convertTo(v));
            }
        }
    }
}
