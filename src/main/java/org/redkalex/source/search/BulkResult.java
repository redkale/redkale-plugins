/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.search;

/**
 *
 * @author zhangjx
 */
public class BulkResult extends BaseBean {

    public long took;

    public boolean errors;

    public BulkItem[] items;

    public int successCount() {
        int count = 0;
        if (items == null) return count;
        for (BulkItem item : items) {
            ActionResult ar = item.action();
            if (ar == null) continue;
            count += ar.successCount();
        }
        return count;
    }

    public static class BulkItem extends BaseBean {

        public ActionResult index;

        public ActionResult create;

        public ActionResult update;

        public ActionResult delete;

        public ActionResult action() {
            if (create != null) return create;
            if (update != null) return update;
            if (delete != null) return delete;
            return index;
        }

    }
}
