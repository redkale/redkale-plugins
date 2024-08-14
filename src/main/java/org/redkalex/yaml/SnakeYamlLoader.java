/*
 *
 */

package org.redkalex.yaml;

import java.util.Map;
import org.redkale.util.AnyValue;
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
        throw new UnsupportedOperationException("Not supported yaml.");
    }
}
