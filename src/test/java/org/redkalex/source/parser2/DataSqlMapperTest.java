/*
 *
 */
package org.redkalex.source.parser2;

import static org.redkale.source.spi.DataSqlMapperBuilder.createMapper;

import org.junit.jupiter.api.Test;
import org.redkale.source.DataJdbcSource;
import org.redkale.source.DataNativeSqlParser;
import org.redkale.source.DataSqlSource;

/** @author zhangjx */
public class DataSqlMapperTest {

	private static DataNativeSqlParser parser = DataNativeSqlParser.loadFirst();

	private static DataSqlSource source = new DataJdbcSource();

	public static void main(String[] args) throws Throwable {
		DataSqlMapperTest test = new DataSqlMapperTest();
		test.run();
	}

	@Test
	public void run() throws Exception {
		ForumInfoMapper mapper = createMapper(parser, source, ForumInfoMapper.class);
		System.out.println(mapper);
	}
}
