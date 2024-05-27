/*
 */
package org.redkalex.source.base;

import org.redkale.convert.json.JsonConvert;
import org.redkale.persistence.*;

/** @author zhangjx */
@Entity
public class TestWorld implements Comparable<TestWorld> {

	@Id
	protected int id;

	protected int randomNumber;

	@Column(nullable = false)
	protected int[] citys;

	public TestWorld() {}

	public TestWorld(int id, int randomNumber) {
		this.id = id;
		this.randomNumber = randomNumber;
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
	public int compareTo(TestWorld o) {
		return Integer.compare(id, o.id);
	}

	public int[] getCitys() {
		return citys;
	}

	public void setCitys(int[] citys) {
		this.citys = citys;
	}

	@Override
	public String toString() {
		return JsonConvert.root().convertTo(this);
	}
}
