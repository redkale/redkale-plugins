/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.util.Objects;

/** @author zhangjx */
public class MyReqPing extends MyReqQuery {

	@SuppressWarnings("OverridableMethodCallInConstructor")
	public MyReqPing() {
		prepare("SELECT 1");
	}

	@Override
	public String toString() {
		return "MyReqPing_" + Objects.hashCode(this) + "{sql=" + sql + "}";
	}
}
