/*
 *
 */
package org.redkalex.source.parser2;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redkale.source.DataJdbcSource;
import org.redkale.source.DataNativeSqlParser;
import org.redkale.source.DataSqlSource;
import static org.redkale.source.spi.DataSqlMapperBuilder.createMapper;

/**
 *
 * @author zhangjx
 */
public class DataSqlMapperTest {

    private static DataNativeSqlParser parser = DataNativeSqlParser.loadFirst();

    private static DataSqlSource source = new DataJdbcSource();

    public static void main(String[] args) throws Throwable {
        DataSqlMapperTest test = new DataSqlMapperTest();
        test.init();
        test.run();
    }

    @BeforeAll
    public static void init() throws Exception {

    }

    @Test
    public void run() throws Exception {
        ForumInfoMapper mapper = createMapper(parser, source, ForumInfoMapper.class);
        System.out.println(mapper);
    }
}
