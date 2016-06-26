/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import org.redkale.service.*;
import org.redkale.util.*;

/**
 *
 * 详情见: http://redkale.org
 *
 * @author zhangjx
 */
@AutoLoad(false)
@LocalService
public class PayService extends AbstractPayService {

    @Override
    public void init(AnyValue config) {
    }

    @Override
    public PayCreatResponse create(PayCreatRequest request) {
        return null;
    }

    @Override
    public PayQueryResponse query(PayRequest request) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PayResponse close(PayRequest request) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PayRefundResponse refund(PayRefundRequest request) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PayRefundQueryResponse queryRefund(PayRequest request) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
