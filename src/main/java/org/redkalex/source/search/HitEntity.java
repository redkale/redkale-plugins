/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.search;

import java.util.Map;

/**
 * @author zhangjx
 * @param <T> T
 */
public class HitEntity<T> extends BaseBean {

    public String _index;

    public String _id;

    public float _score;

    public T _source;

    public Map<String, String[]> highlight;
}
