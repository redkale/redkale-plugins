package org.redkalex.test.rest;

import java.util.List;
import javax.annotation.Resource;

import org.redkale.service.*;
import org.redkale.source.DataSource;
import org.redkale.source.Flipper;
import org.redkale.util.Sheet;
import org.redkalex.rest.*;

@RestController(value = "hello", module = 200, ignore = false, repair = true)
public class HelloService implements Service {

    @Resource
    private DataSource source;

    @RestMapping(name = "query", authignore = true, actionid = 2001)
    public Sheet<HelloEntity> queryHello(@RestParam("bean") HelloBean bean, Flipper flipper) {
        return source.querySheet(HelloEntity.class, flipper, bean);
    }

    @RestMapping(name = "list", authignore = true, actionid = 2001)
    public List<HelloEntity> queryHello(@RestParam("bean") HelloBean bean) {
        return source.queryList(HelloEntity.class, bean);
    }

    @RestMapping(name = "jsfind", authignore = true, actionid = 2001, jsvar = "varhello")
    @RestMapping(name = "find", authignore = true, actionid = 2001)
    public HelloEntity findHello(@RestParam("#") int id) { //#表示直接跟在url后面  如: /hello/find/123
        return source.find(HelloEntity.class, id);
    }

    public String findHelloName(@RestParam("#") int id) { //#表示直接跟在url后面  如: /hello/find/123
        HelloEntity entity = source.find(HelloEntity.class, id);
        return entity == null ? null : entity.getHelloname();
    }

    public long findHelloTime(@RestParam("#") int id) { //#表示直接跟在url后面  如: /hello/find/123
        HelloEntity entity = source.find(HelloEntity.class, id);
        return entity == null ? 0 : entity.getCreatetime();
    }
    
    @RestMapping(name = "create", authignore = false, actionid = 2002)
    public RetResult<HelloEntity> createHello(@RestParam("bean") HelloEntity entity) {
        source.insert(entity);
        return new RetResult<>(entity);
    }

    @RestMapping(name = "update", authignore = false, actionid = 2003)
    public void updateHello(@RestParam("bean") HelloEntity entity) {
        source.update(entity);
    }

    @RestMapping(name = "partupdate", authignore = false, actionid = 2003) //不能用updatepart 因为updatepart包含update开头
    public void updateHello(UserInfo user, @RestParam("bean") HelloEntity entity, @RestParam("columns") String... columns) {
        source.updateColumns(entity, columns);
    }

    @RestMapping(name = "delete", authignore = false, actionid = 2004)
    public void deleteHello(@RestParam("#") int id) {
        source.delete(HelloEntity.class, id);
    }

    @RestMapping(name = "remove", authignore = false, actionid = 2004)
    public void deleteHello(@RestParam("bean") HelloEntity entity) {
        source.delete(entity);
    }
}
