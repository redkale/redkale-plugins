/*
 */
package org.redkalex.pay;

import org.redkale.util.RedkaleException;

/**
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayRefundQryReq extends PayRequest {

    // 微信支付: 商户系统内部唯一，同一退款单号多次请求只退一笔
    // 支付宝： 标识一次退款请求，同一笔交易多次退款需要保证唯一，如需部分退款，则此参数必传
    protected String refundno = ""; // 退款编号 商户系统内部的退款单号。

    @Override
    public void checkVaild() {
        super.checkVaild();
        if (this.refundno == null || this.refundno.isEmpty()) {
            throw new RedkaleException("refundno is illegal");
        }
    }

    public String getRefundno() {
        return refundno;
    }

    public void setRefundno(String refundno) {
        this.refundno = refundno;
    }
}
