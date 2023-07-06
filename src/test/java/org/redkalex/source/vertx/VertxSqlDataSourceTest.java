/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.vertx;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.persistence.*;
import org.redkale.source.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class VertxSqlDataSourceTest {

    private static final Random random = new SecureRandom();

    protected static int randomId() {
        return random.nextInt(10000) + 1;
    }

    public static void main(String[] args) throws Throwable {

        Properties prop = new Properties();
        prop.setProperty("redkale.datasource[].url", "jdbc:postgresql://127.0.0.1:5432/hello_world"); //192.168.175.1  127.0.0.1 192.168.1.103
        prop.setProperty("redkale.datasource[].preparecache", "true");
        prop.setProperty("redkale.datasource[].table-autoddl", "true");
        prop.setProperty("redkale.datasource[].user", "postgres");
        prop.setProperty("redkale.datasource[].password", "1234");

        final VertxSqlDataSource source = new VertxSqlDataSource();
        source.init(AnyValue.loadFromProperties(prop).getAnyValue("redkale").getAnyValue("datasource").getAnyValue(""));
        System.out.println(source.find(Fortune.class, 5));
        System.out.println(source.queryList(Fortune.class));
        System.out.println("findsList: " + source.findsList(Fortune.class, List.of(2, 3, 1, 0, 30).stream()));
        Fortune one = source.queryList(Fortune.class).get(0);
        one.setMessage(one.getMessage() + " zz");
        System.out.println(source.updateColumn(one, "message"));
        one.setMessage(one.getMessage() + " zz");
        System.out.println(source.update(one));
        System.out.println("运行完成");
        source.destroy(null);
    }

    protected static World[] sort(World[] worlds) {
        Arrays.sort(worlds);
        return worlds;
    }

    protected static ByteBuffer writeUTF8String(ByteBuffer array, String string) {
        array.put(string.getBytes(StandardCharsets.UTF_8));
        array.put((byte) 0);
        return array;
    }

    protected static String readUTF8String(ByteBuffer buffer, ByteArray array) {
        int i = 0;
        array.clear();
        for (byte c = buffer.get(); c != 0; c = buffer.get()) {
            array.put(c);
        }
        return array.toString(StandardCharsets.UTF_8);
    }

    //@DistributeTable(strategy = Record.TableStrategy.class)
    @Entity
    public static class Record {

        public static class TableStrategy implements DistributeTableStrategy<Record> {

            private static final String format = "%1$tY%1$tm";

            @Override
            public String[] getTables(String table, FilterNode node) {
                int pos = table.indexOf('.');
                return new String[]{table.substring(pos + 1) + "_" + String.format(format, System.currentTimeMillis())};
            }

            @Override
            public String getTable(String table, Record bean) {
                int pos = table.indexOf('.');
                return table.substring(pos + 1) + "_" + String.format(format, System.currentTimeMillis());
            }

            @Override
            public String getTable(String table, Serializable primary) {
                int pos = table.indexOf('.');
                return table.substring(pos + 1) + "_" + String.format(format, System.currentTimeMillis());
            }
        }

        @Id
        private int id;

        private String name = "";

        public Record() {
        }

        public Record(String name) {
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }

    }

    @Entity
    public static class Fortune implements Comparable<Fortune> {

        @Id
        private int id;

        private String message = "";

        public Fortune() {
        }

        public Fortune(int id, String message) {
            this.id = id;
            this.message = message;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        @Override
        public int compareTo(Fortune o) {
            return message.compareTo(o.message);
        }

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }

    }

    @Entity
    public static class World implements Comparable<World> {

        @Id
        private int id;

        private int randomNumber;

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

}
