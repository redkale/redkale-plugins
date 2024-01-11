/*
 *
 */
package org.redkalex.source.parser;

/**
 *
 * @author zhangjx
 */
public class NativeSqlFragment {

    //是否是${xx.xx}参数
    private final boolean dollarable;

    //sql语句片段或者${}参数名或者:jdbcNamedParameter
    private final String text;

    public NativeSqlFragment(boolean signable, String text) {
        this.dollarable = signable;
        this.text = text;
    }

    public boolean isDollarable() {
        return dollarable;
    }

    public String getText() {
        return text;
    }

    @Override
    public String toString() {
        return dollarable ? ("${" + text + "}") : text;
    }
}
