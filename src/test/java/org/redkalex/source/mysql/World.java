/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import javax.persistence.*;
import org.redkale.convert.json.JsonConvert;

/**
 *
 * @author zhangjx
 */
@Entity
public class World implements Comparable<World> {

    @Id
    protected int id;

    protected int randomNumber;

    public World randomNumber(int randomNumber) {
        this.randomNumber = randomNumber;
        return this;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getRandomNumber() {
        return randomNumber;
    }

    public void setRandomNumber(int randomNumber) {
        this.randomNumber = randomNumber;
    }

    @Override
    public int compareTo(World o) {
        return Integer.compare(id, o.id);
    }

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }

}
