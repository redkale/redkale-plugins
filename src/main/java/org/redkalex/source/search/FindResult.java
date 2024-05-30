/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.search;

/**
 * @author zhangjx
 * @param <T> T
 */
public class FindResult<T> extends BaseBean {

    public boolean found;

    public int _version;

    public T _source;
}
