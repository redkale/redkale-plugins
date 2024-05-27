/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.Map;
import org.redkale.convert.*;

/**
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayPreResponse extends PayResponse {

	public static final String PREPAY_PREPAYID = "prepayid";

	public static final String PREPAY_PAYURL = "payurl";

	@ConvertColumn(ignore = true, type = ConvertType.JSON)
	private String appid = "";

	private String thirdPayno = ""; // 第三方的支付流水号

	@Override
	public PayPreResponse retcode(int retcode) {
		this.retcode = retcode;
		this.retinfo = PayRetCodes.retInfo(retcode);
		return this;
	}

	@Override
	public PayPreResponse retinfo(String retinfo) {
		if (retinfo != null) this.retinfo = retinfo;
		return this;
	}

	@Override
	public PayPreResponse result(Map<String, String> result) {
		this.setResult(result);
		return this;
	}

	public String getThirdPayno() {
		return thirdPayno;
	}

	public void setThirdPayno(String thirdPayno) {
		this.thirdPayno = thirdPayno;
	}

	public String getAppid() {
		return appid == null ? "" : appid;
	}

	public void setAppid(String appid) {
		this.appid = appid;
	}

	@Deprecated
	@ConvertDisabled
	public String getThirdpayno() {
		return thirdPayno;
	}

	@Deprecated
	@ConvertDisabled
	public void setThirdpayno(String thirdPayno) {
		this.thirdPayno = thirdPayno;
	}
}
