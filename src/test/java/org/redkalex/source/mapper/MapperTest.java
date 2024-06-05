/*

*/

package org.redkalex.source.mapper;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redkale.source.DataJdbcSource;
import org.redkale.source.DataNativeSqlParser;
import org.redkale.source.DataSqlSource;
import org.redkale.source.spi.DataSqlMapperBuilder;

/**
 *
 * @author zhangjx
 */
public class MapperTest {

    private static DataSqlSource source = new DataJdbcSource();

    public static void main(String[] args) throws Throwable {
        MapperTest test = new MapperTest();
        test.init();
        test.run();
    }

    @BeforeAll
    public static void init() throws Exception {
        // do
    }

    @Test
    public void run() throws Exception {
        DataNativeSqlParser nativeSqlParser = DataNativeSqlParser.loadFirst();
        DataSqlMapperBuilder.createMapper(nativeSqlParser, source, ForumInfoMapper.class);
    }
}
