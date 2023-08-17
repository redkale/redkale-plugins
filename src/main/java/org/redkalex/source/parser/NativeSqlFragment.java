/*
 *
 */
package org.redkalex.source.parser;

/**
 *
 * @author zhangjx
 */
public class NativeSqlFragment {

    //是否是参数
    private final boolean paramable;

    //sql语句片段或者#{}参数名
    private final String text;

    public NativeSqlFragment(boolean paramable, String text) {
        this.paramable = paramable;
        this.text = text;
    }

    public boolean isParamable() {
        return paramable;
    }

    public String getText() {
        return text;
    }

    @Override
    public String toString() {
        return paramable ? ("#{" + text + "}") : text;
    }
}
