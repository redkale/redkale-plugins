/*
 *
 */
package org.redkalex.source.pgsql;

import java.util.Properties;
import org.redkale.inject.ResourceFactory;
import org.redkale.source.DataJdbcSource;
import org.redkale.util.*;
import org.redkalex.source.base.IncreWorld;

/** @author zhangjx */
public class PgSQLJdbcTest {

	private static final String url = "jdbc:postgresql://127.0.0.1:5432/hello_world";

	private static final String user = "postgres";

	private static final String password = "1234";

	public static void main(String[] args) throws Throwable {
		Properties prop = new Properties();
		prop.setProperty("redkale.datasource.default.url", url);
		prop.setProperty("redkale.datasource.default.table-autoddl", "true");
		prop.setProperty("redkale.datasource.default.user", user);
		prop.setProperty("redkale.datasource.default.password", password);

		ResourceFactory factory = ResourceFactory.create();
		DataJdbcSource source = new DataJdbcSource();
		factory.inject(source);
		source.init(AnyValue.loadFromProperties(prop)
				.getAnyValue("redkale")
				.getAnyValue("datasource")
				.getAnyValue("default"));
		System.out.println("---------");

		source.dropTable(IncreWorld.class);
		IncreWorld in1 = new IncreWorld();
		in1.setRandomNumber(11);
		IncreWorld in2 = new IncreWorld();
		in2.setRandomNumber(22);
		source.insert(in1, in2);
		System.out.println("IncreWorld记录: " + in1);
		System.out.println("IncreWorld记录: " + in2);

		System.out.println("---------------- 准备关闭DataSource ----------------");
		source.close();
		System.out.println("---------------- 全部执行完毕 ----------------");
	}
}
