/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import org.redkale.service.Service;

/**
 *
 * 详情见: http://redkale.org
 *
 * @author zhangjx
 */
abstract class AbstractPayService implements Service {

    public abstract PayResponse create(PayCreatRequest request);

    public abstract PayQueryResponse query(PayRequest request);

    public abstract PayResponse close(PayRequest request);

    public abstract PayRefundResponse refund(PayRefundRequest request);

    public abstract PayRefundQueryResponse queryRefund(PayRequest request);
}
