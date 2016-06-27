package org.redkalex.test.rest;

import javax.persistence.Id;
import org.redkale.convert.json.JsonFactory;

public class HelloEntity {

    @Id
    private int helloid;

    private String helloname;

    private long createtime;

    public int getHelloid() {
        return helloid;
    }

    public void setHelloid(int helloid) {
        this.helloid = helloid;
    }

    public String getHelloname() {
        return helloname;
    }

    public void setHelloname(String helloname) {
        this.helloname = helloname;
    }

    public long getCreatetime() {
        return createtime;
    }

    public void setCreatetime(long createtime) {
        this.createtime = createtime;
    }

    @Override
    public String toString() {
        return JsonFactory.root().getConvert().convertTo(this);
    }
}
