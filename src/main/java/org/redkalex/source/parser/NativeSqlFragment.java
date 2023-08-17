/*
 *
 */
package org.redkalex.source.parser;

/**
 *
 * @author zhangjx
 */
public class NativeSqlFragment {

    //是否是#{xx.xx}参数
    private final boolean signable;

    //sql语句片段或者#{}参数名
    private final String text;

    public NativeSqlFragment(boolean signable, String text) {
        this.signable = signable;
        this.text = text;
    }

    public boolean isSignable() {
        return signable;
    }

    public String getText() {
        return text;
    }

    @Override
    public String toString() {
        return signable ? ("#{" + text + "}") : text;
    }
}
