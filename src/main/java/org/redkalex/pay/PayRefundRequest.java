/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.*;
import org.redkale.convert.ConvertDisabled;
import org.redkale.util.RedkaleException;

/**
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayRefundRequest extends PayRequest {

	protected long payMoney; //  支付金额。 单位:分

	protected String payCurrency; //  币种,一般币值*100

	protected String thirdPayno = ""; // 第三方的支付流水号

	protected long refundMoney; //  退款金额。 单位:分  不能大于支付金额

	// 微信支付: 商户系统内部唯一，同一退款单号多次请求只退一笔
	// 支付宝： 标识一次退款请求，同一笔交易多次退款需要保证唯一，如需部分退款，则此参数必传
	protected String refundno = ""; // 退款编号 商户系统内部的退款单号。

	protected String clientHost = ""; // HTTP请求的Host

	protected String clientAddr = ""; // 客户端IP地址

	protected Map<String, String> attach; // 扩展信息

	@Override
	public void checkVaild() {
		super.checkVaild();
		if (this.refundMoney < 1) {
			throw new RedkaleException("refundMoney is illegal");
		}
		if (this.payMoney < 1) {
			throw new RedkaleException("payMoney is illegal");
		}
		if (this.refundno == null || this.refundno.isEmpty()) {
			throw new RedkaleException("refundno is illegal");
		}
		if (this.thirdPayno == null || this.thirdPayno.isEmpty()) {
			throw new RedkaleException("thirdPayno is illegal");
		}
		if (this.clientAddr == null || this.clientAddr.isEmpty()) {
			throw new RedkaleException("clientAddr is illegal");
		}
	}

	public Map<String, String> attach(String key, Object value) {
		if (this.attach == null) {
			this.attach = new TreeMap<>();
		}
		this.attach.put(key, String.valueOf(value));
		return this.attach;
	}

	public String getAttach(String name) {
		return attach == null ? null : attach.get(name);
	}

	public String getAttach(String name, String defValue) {
		return attach == null ? defValue : attach.getOrDefault(name, defValue);
	}

	@Deprecated
	@ConvertDisabled
	public long getRefundmoney() {
		return refundMoney;
	}

	@Deprecated
	@ConvertDisabled
	public void setRefundmoney(long refundMoney) {
		this.refundMoney = refundMoney;
	}

	@Deprecated
	@ConvertDisabled
	public long getPaymoney() {
		return payMoney;
	}

	@Deprecated
	@ConvertDisabled
	public void setPaymoney(long payMoney) {
		this.payMoney = payMoney;
	}

	@Deprecated
	@ConvertDisabled
	public String getPaycurrency() {
		return payCurrency;
	}

	@Deprecated
	@ConvertDisabled
	public void setPaycurrency(String payCurrency) {
		this.payCurrency = payCurrency;
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

	@Deprecated
	@ConvertDisabled
	public String getClienthost() {
		return clientHost;
	}

	@Deprecated
	@ConvertDisabled
	public void setClienthost(String clientHost) {
		this.clientHost = clientHost;
	}

	public long getRefundMoney() {
		return refundMoney;
	}

	public void setRefundMoney(long refundMoney) {
		this.refundMoney = refundMoney;
	}

	public long getPayMoney() {
		return payMoney;
	}

	public void setPayMoney(long payMoney) {
		this.payMoney = payMoney;
	}

	public String getPayCurrency() {
		return payCurrency;
	}

	public void setPayCurrency(String payCurrency) {
		this.payCurrency = payCurrency;
	}

	public String getRefundno() {
		return refundno;
	}

	public void setRefundno(String refundno) {
		this.refundno = refundno;
	}

	public String getThirdPayno() {
		return thirdPayno;
	}

	public void setThirdPayno(String thirdPayno) {
		this.thirdPayno = thirdPayno;
	}

	public String getClientHost() {
		return clientHost;
	}

	public void setClientHost(String clientHost) {
		this.clientHost = clientHost;
	}

	public String getClientAddr() {
		return clientAddr;
	}

	public void setClientAddr(String clientAddr) {
		this.clientAddr = clientAddr;
	}

	public Map<String, String> getAttach() {
		return attach;
	}

	public void setAttach(Map<String, String> attach) {
		this.attach = attach;
	}

	@Deprecated
	public Map<String, String> add(String key, String value) {
		if (this.attach == null) {
			this.attach = new TreeMap<>();
		}
		this.attach.put(key, value);
		return this.attach;
	}

	@Deprecated
	public String attach(String name) {
		return attach == null ? null : attach.get(name);
	}

	@Deprecated
	public String attach(String name, String defValue) {
		return attach == null ? defValue : attach.getOrDefault(name, defValue);
	}

	@Deprecated
	public String getMapValue(String name) {
		return attach == null ? null : attach.get(name);
	}

	@Deprecated
	public String getMapValue(String name, String defValue) {
		return attach == null ? defValue : attach.getOrDefault(name, defValue);
	}

	@Deprecated
	public Map<String, String> getMap() {
		return attach;
	}

	@Deprecated
	public void setMap(Map<String, String> map) {
		this.attach = map;
	}
}
