/*
 *
 */
package org.redkalex.source.base;

import org.redkale.convert.json.JsonConvert;
import org.redkale.persistence.*;

/**
 *
 * @author zhangjx
 */
@Entity
public class IncreWorld {

    @Id
    @GeneratedValue
    protected int id;

    protected Integer randomNumber;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Integer getRandomNumber() {
        return randomNumber;
    }

    public void setRandomNumber(Integer randomNumber) {
        this.randomNumber = randomNumber;
    }

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }

}
