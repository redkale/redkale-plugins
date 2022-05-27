/*
 */
package org.redkalex.source.base;

import org.redkale.source.*;

/**
 *
 * @author zhangjx
 */
public class SourceTest {

    public static void run(DataSource source) throws Exception {
        final long now = 1640966400000L;
        source.dropTable(TestWorld.class);
        source.dropTable(BeanRecord.class);
        source.dropTable(BeanRecord.class, FilterNode.create("createTime", now));
        int count = 100;
        int limit = 20;
        int rs;
        TestWorld[] worlds = new TestWorld[count];
        BeanRecord[] records = new BeanRecord[count];
//        if (source.updateColumn(TestRecord.class, FilterNode.create("createTime", now), ColumnValue.mov("content", "haha")) != -1) {
//            System.err.println("更新数量应该是-1");
//        }
        for (int i = 0; i < count; i++) {
            worlds[i] = new TestWorld(i + 1, i + 1);
            if (i == 38) worlds[i].setCitys(new int[]{1, 2});
            records[i] = new BeanRecord();
            records[i].setRecordsid(now + "-" + (i + 1));
            records[i].setCreateTime(now);
            records[i].setImg(new byte[]{(byte) 'A', (byte) 'B'});
        }
        System.out.println("开始新增world");
        rs = source.insert(worlds);
        if (rs != worlds.length) {
            new Exception("TestWorld新增数量应该是" + worlds.length + ", 却是:" + rs).printStackTrace();
            return;
        }
        System.out.println("开始新增record");
        rs = source.insert(records);
        if (rs != records.length) {
            new Exception("BeanRecord新增数量应该是" + records.length + ", 却是:" + rs).printStackTrace();
            return;
        }
        System.out.println("查找单个record");
        TestWorld r = source.find(TestWorld.class, worlds[38].getId());
        if (r == null) {
            new Exception("应该有值:" + worlds[38] + ", 却是:" + null).printStackTrace();
            return;
        }
        System.out.println("更新单个record");
        rs = source.updateColumn(worlds[39], "citys");
        if (rs != 1) {
            new Exception("更新数量应该是" + 1 + ", 却是:" + rs).printStackTrace();
            return;
        }
        rs = source.updateColumn(TestWorld.class, worlds[39].getId(), "citys", null);
        if (rs != 1) {
            new Exception("更新数量应该是" + 1 + ", 却是:" + rs).printStackTrace();
            return;
        }
        rs = source.updateColumn(TestWorld.class, worlds[39].getId(), "citys", new int[]{5, 6});
        if (rs != 1) {
            new Exception("更新数量应该是" + 1 + ", 却是:" + rs).printStackTrace();
            return;
        }
        source.find(TestWorld.class, worlds[39].getId());

        worlds[40].setCitys(new int[]{1, 2, 3, 4});
        rs = source.updateColumn(worlds[40], "citys");
        if (rs != 1) {
            new Exception("更新数量应该是" + 1 + ", 却是:" + rs).printStackTrace();
            return;
        }
        System.out.println("开始删除");
        rs = source.delete(TestWorld.class, new Flipper(limit), FilterNode.create("randomNumber", FilterExpress.GREATERTHANOREQUALTO, 1));
        if (rs != limit) {
            new Exception("删除数量应该是" + limit + ", 却是:" + rs).printStackTrace();
            return;
        }
        System.out.println("开始更新到1000");
        rs = source.updateColumn(TestWorld.class, FilterNode.create("randomNumber", FilterExpress.GREATERTHANOREQUALTO, 1), new Flipper(limit), ColumnValue.mov("randomNumber", 1000));
        if (rs != limit) {
            new Exception("更新数量应该是" + limit + ", 却是:" + rs).printStackTrace();
            return;
        }
        System.out.println("获取更新后的数量");
        rs = source.getNumberResult(TestWorld.class, FilterFunc.COUNT, "id", FilterNode.create("randomNumber", 1000)).intValue();
        if (rs != limit) {
            new Exception("更新数量应该是" + limit + ", 却是:" + rs).printStackTrace();
            return;
        }
        TestWorld one = source.find(TestWorld.class, limit + limit / 2);
        one.setRandomNumber(999);
        rs = source.update(one);
        if (rs != 1) {
            new Exception("更新数量应该是" + 1 + ", 却是:" + rs).printStackTrace();
            return;
        }
        rs = source.getNumberResult(TestWorld.class, FilterFunc.COUNT, "id", FilterNode.create("randomNumber", FilterExpress.NOTEQUAL, 1000)).intValue();
        if (rs != (count - limit - limit + 1)) {
            new Exception("一共数量应该是" + (count - limit - limit + 1) + ", 却是:" + rs).printStackTrace();
            return;
        }
        BeanRecord record = source.find(BeanRecord.class, now + "-" + 1);
        record.setImg(new byte[]{(byte) 'A', (byte) 'B', (byte) 'C'});
        rs = source.update(record);
        if (rs != 1) {
            new Exception("更新数量应该是" + 1 + ", 却是:" + rs).printStackTrace();
            return;
        }
        record.setImg(new byte[]{(byte) 'A', (byte) 'B', (byte) 'C', (byte) 'D'});
        rs = source.updateColumn(record, "img");
        if (rs != 1) {
            new Exception("更新数量应该是" + 1 + ", 却是:" + rs).printStackTrace();
            return;
        }
        System.out.println("运行结束");
    }
}
