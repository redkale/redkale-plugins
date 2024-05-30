/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.search;

/** @author zhangjx */
public class ActionResult extends BaseBean {

    public String _index;

    public String _id;

    public int _version;

    public String result;

    public ShardResult _shards;

    public int status;

    public int _primary_term;

    public int successCount() {
        return _shards == null ? 0 : _shards.successful;
    }
}
