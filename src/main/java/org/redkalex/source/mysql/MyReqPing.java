/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

/**
 *
 * @author zhangjx
 */
public class MyReqPing extends MyReqQuery {

    public static final MyReqPing INSTANCE = new MyReqPing();

    @SuppressWarnings("OverridableMethodCallInConstructor")
    public MyReqPing() {
        prepare("SELECT 1");
    }
}
