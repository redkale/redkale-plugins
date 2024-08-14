/*
 *
 */

package org.redkalex.yaml;

import org.redkale.util.YamlProvider;

/**
 *
 * @author zhangjx
 */
public class SnakeYamlProvider implements YamlProvider {

    @Override
    public YamlLoader createLoader() {
        try {
            // 尝试加载snakeyaml
            org.yaml.snakeyaml.Yaml.class.isAssignableFrom(Object.class);
            return new SnakeYamlLoader();
        } catch (Throwable t) {
            return null;
        }
    }
}
