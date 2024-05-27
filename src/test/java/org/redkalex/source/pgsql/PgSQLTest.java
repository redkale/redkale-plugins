/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import static org.redkale.boot.Application.RESNAME_APP_CLIENT_ASYNCGROUP;
import static org.redkale.source.DataSources.*;

import java.io.Serializable;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BiConsumer;
import java.util.stream.*;
import org.redkale.boot.LoggingBaseHandler;
import org.redkale.convert.json.JsonConvert;
import org.redkale.inject.ResourceFactory;
import org.redkale.net.*;
import org.redkale.net.client.Client;
import org.redkale.persistence.*;
import org.redkale.source.*;
import org.redkale.util.*;
import org.redkalex.source.base.IncreWorld;
import org.redkalex.source.parser.DataNativeJsqlParser;
import org.redkalex.source.vertx.TestRecord;

/** @author zhangjx */
public class PgSQLTest {

	private static final Random random = new SecureRandom();

	private static final String url = "jdbc:postgresql://127.0.0.1:5432/hello_world";

	private static final String user = "postgres";

	private static final String password = "1234";

	private static final int count = Utility.cpus(); // Runtime.getRuntime().availableProcessors() * 10;  //4.18秒

	public static void main(String[] args) throws Throwable {
		LoggingBaseHandler.initDebugLogConfig();
		// run(false, true);
		run(true, false);
		// run(true, true);
		// run(true, false);
	}

	public static void run(final boolean forFortune, final boolean rwSeparate) throws Throwable {
		final AsyncIOGroup asyncGroup = new AsyncIOGroup(8192, 16);
		asyncGroup.start();
		ResourceFactory factory = ResourceFactory.create();
		factory.register(RESNAME_APP_CLIENT_ASYNCGROUP, asyncGroup);
		factory.register("", new DataNativeJsqlParser());

		final PgsqlDataSource source = new PgsqlDataSource();
		PgsqlDataSource.debug = false;

		Properties prop = new Properties();
		if (rwSeparate) { // 读写分离
			prop.setProperty("redkale.datasource.default.read.url", url);
			prop.setProperty("redkale.datasource.default.read.table-autoddl", "true");
			prop.setProperty("redkale.datasource.default.read.user", user);
			prop.setProperty("redkale.datasource.default.read.password", password);

			prop.setProperty("redkale.datasource.default.write.url", url);
			prop.setProperty("redkale.datasource.default.write.table-autoddl", "true");
			prop.setProperty("redkale.datasource.default.write.user", user);
			prop.setProperty("redkale.datasource.default.write.password", password);
		} else {
			prop.setProperty("redkale.datasource.default.url", url);
			prop.setProperty("redkale.datasource.default.table-autoddl", "true");
			prop.setProperty("redkale.datasource.default.user", user);
			prop.setProperty("redkale.datasource.default.password", password);
		}
		factory.inject(source);
		source.init(AnyValue.loadFromProperties(prop)
				.getAnyValue("redkale")
				.getAnyValue("datasource")
				.getAnyValue("default"));
		System.out.println("-------------------- " + (forFortune ? "Fortune" : "World") + " "
				+ (rwSeparate ? "读写分离" : "读写合并") + " --------------------");
		System.out.println("-------------------- " + "当前内核数: " + Utility.cpus() + " --------------------");
		{
			// source.dropTable(TestRecord.class);
			TestRecord entity = new TestRecord();
			entity.setRecordid("r223" + System.currentTimeMillis());
			entity.setScore(200);
			entity.setStatus((short) 10);
			entity.setName("myname2");
			entity.setCreateTime(System.currentTimeMillis());
			source.insert(entity);

			Map<String, Object> params =
					Utility.ofMap("name", "%", "s", 10, "ids", Utility.ofList(entity.getRecordid()));
			String sql = "SELECT * FROM TestRecord WHERE name LIKE :name OR recordid IN :ids";
			TestRecord one = source.nativeQueryOne(TestRecord.class, sql, params);
			System.out.println(one);

			String upsql = "UPDATE TestRecord SET name='aa' WHERE name LIKE :name OR status = :s OR recordid IN :ids";
			int rs = source.nativeUpdate(upsql, params);
			System.out.println("修改结果数: " + rs);
			System.out.println(source.find(TestRecord.class, entity.getRecordid()));

			String sheetSql = "SELECT * FROM TestRecord WHERE name LIKE :name OR recordid IN :ids";
			Flipper flipper = new Flipper(2);
			Sheet<TestRecord> sheet = source.nativeQuerySheet(TestRecord.class, sheetSql, flipper, params);
			System.out.println(sheet);
			System.out.println("获得总数: " + sheet.getTotal());

			sheetSql = "SELECT * FROM TestRecord WHERE recordid IN :ids";
			sheet = source.nativeQuerySheet(TestRecord.class, sheetSql, flipper, params);
			System.out.println(sheet);
			System.out.println("获得总数: " + sheet.getTotal());
		}
		if (true) {
			System.out.println("当前机器CPU核数: " + Utility.cpus());
			System.out.println(
					"随机获取World记录1: " + source.findAsync(World.class, randomId()).join());
			System.out.println("随机获取World记录2: "
					+ source.findsListAsync(World.class, Stream.of(randomId(), -1122, randomId()))
							.join());
			System.out.println("随机获取World记录3: "
					+ Arrays.toString(source.findsAsync(World.class, randomId(), -1122, randomId())
							.join()));
			World w1 = source.findAsync(World.class, 11).join();
			World w2 = source.findAsync(World.class, 22).join();
			System.out.println("随机获取World记录4: " + w1 + ", " + w2);
			w1.setRandomNumber(w1.getRandomNumber() + 2);
			w2.setRandomNumber(w2.getRandomNumber() + 2);
			source.updateAsync(w1, w2).join();
			w1 = source.findAsync(World.class, 11).join();
			w2 = source.findAsync(World.class, 22).join();
			System.out.println("修改后World记录: " + w1 + ", " + w2);

			System.out.println("随机获取World记录5: "
					+ source.findsListAsync(World.class, Stream.of(1, 2, 3, 4, 5))
							.join());

			IntStream ids = IntStream.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
			System.out.println("组合操作1: "
					+ source.findsListAsync(World.class, ids.boxed())
							.thenCompose(words -> source.updateAsync(World.modifyNumber(words))
									.thenApply(v -> words))
							.join());

			ids = IntStream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
			System.out.println("组合操作2: "
					+ source.findsListAsync(World.class, ids.boxed())
							.thenCompose(words -> source.updateAsync(World.modifyNumber(words))
									.thenApply(v -> words))
							.join());

			System.out.println("查询模板数据: " + source.queryList(Fortune.class));

			source.nativeQuery("SELECT COUNT(*) FROM World", rs -> {
				System.out.println("获得World总数: " + (rs.next() ? rs.getObject(1) : null));
				return null;
			});
			System.out.println("执行结果应该是(3): " + source.nativeUpdate("UPDATE World SET randomNumber = id WHERE id < 3"));

			final CompletableFuture[] futures = new CompletableFuture[Utility.cpus()];
			for (int i = 0; i < futures.length; i++) {
				futures[i] = source.findAsync(World.class, randomId()).thenCompose(v -> source.updateAsync(v));
			}
			CompletableFuture.allOf(futures).join();
			System.out.println("已连接数: "
					+ prop.getProperty(
							DATA_SOURCE_MAXCONNS, "" + Runtime.getRuntime().availableProcessors()));

			System.out.println("只读池req发送数: " + getWriteReqCounter(source.readPool()));
			System.out.println("只读池resp处理数: " + getPollRespCounter(source.readPool()));
			System.out.println("只读池resp等待数: " + getRespWaitingCount(source.readPool()));
			if (source.readPool() != source.writePool()) {
				System.out.println("可写池req发送数: " + getWriteReqCounter(source.writePool()));
				System.out.println("可写池resp处理数: " + getPollRespCounter(source.writePool()));
				System.out.println("可写池resp等待数: " + getRespWaitingCount(source.writePool()));
			}
		}
		source.dropTable(IncreWorld.class);
		IncreWorld in1 = new IncreWorld();
		in1.setRandomNumber(11);
		IncreWorld in2 = new IncreWorld();
		in2.setRandomNumber(22);
		System.out.println("准备新增");
		source.insert(in1, in2);
		System.out.println("IncreWorld记录: " + in1);
		System.out.println("IncreWorld记录: " + in2);

		// System.out.println(source.findsList(Fortune.class, List.of(1, 222, 2, 3).stream()));
		source.queryList(Fortune.class);
		final int fortuneSize = source.queryList(Fortune.class).size();
		System.out.println("Fortune数量: " + fortuneSize);

		//        for (int i = 1; i <= 10000; i++) {
		//            if (!source.exists(World.class, i)) {
		//                World w = new World();
		//                w.id = i;
		//                w.randomNumber = i;
		//                source.insert(w);
		//            }
		//        }
		//        CompletableFuture[] ffs = new CompletableFuture[2];
		//        ffs[0] = source.findAsync(Fortune.class, 1);
		//        ffs[1] = source.findAsync(Fortune.class, 2);
		//        CompletableFuture.allOf(ffs).join();
		//        System.out.println(ffs[0].join());
		//        System.out.println(ffs[1].join());
		//        ffs = new CompletableFuture[2];
		//        ffs[0] = source.findAsync(Fortune.class, 1);
		//        ffs[1] = source.findAsync(Fortune.class, 2);
		//        CompletableFuture.allOf(ffs).join();
		//        System.out.println(ffs[0].join());
		//        System.out.println(ffs[1].join());
		System.out.println("-------------------- 压测开始 --------------------");
		getWriteReqCounter(source.readPool()).reset();
		getPollRespCounter(source.readPool()).reset();
		getWriteReqCounter(source.writePool()).reset();
		getPollRespCounter(source.writePool()).reset();
		long s1 = System.currentTimeMillis();
		final AtomicInteger timeouts = new AtomicInteger();
		Field errorCompleteConsumer = AbstractDataSqlSource.class.getDeclaredField("errorCompleteConsumer");
		errorCompleteConsumer.setAccessible(true);
		BiConsumer<Object, Throwable> bc = (Object r, Throwable t) -> {
			if (t == null) {
				return;
			}
			if (t.getCause() instanceof TimeoutException) {
				timeouts.incrementAndGet();
			} else {
				t.printStackTrace();
			}
		};

		final ExecutorService executor = WorkThread.createExecutor(count, "Test-WorkThread-%s");
		final CountDownLatch cdl = new CountDownLatch(count);
		final CountDownLatch startcdl = new CountDownLatch(count);
		errorCompleteConsumer.set(source, bc);
		for (int j = 0; j < count; j++) {
			executor.execute(() -> {
				try {
					startcdl.countDown();
					startcdl.await();

					final World[] rs = new World[3];
					final CompletableFuture[] futures = new CompletableFuture[rs.length];
					for (int i = 0; i < rs.length; i++) {
						final int index = i;
						int id = randomId();
						IntStream ids = ThreadLocalRandom.current().ints(20, 1, 10001);
						futures[index] = forFortune
								? source.queryListAsync(Fortune.class)
								: source.findsListAsync(World.class, ids.boxed())
										.thenApply(v -> {
											if (v.size() != 20) {
												System.out.println("数量居然是" + v.size());
											}
											return v;
										});
					}
					CompletableFuture.allOf(futures)
							.thenCompose(v -> {
								if (forFortune) {
									List s = (List) futures[0].join();
									if (s.size() != fortuneSize) {
										System.out.println("数量居然是" + s.size());
									}
									return CompletableFuture.completedFuture(null);
								}
								return CompletableFuture.completedFuture(null);
								// return source.updateAsync(sort(rs));
							})
							.whenComplete((r, t) -> {
								cdl.countDown();
								if (t != null) {
									if (t.getCause() instanceof TimeoutException) {
										timeouts.incrementAndGet();
									} else {
										t.printStackTrace();
									}
								}
							});
				} catch (Throwable t) {
					if (t.getCause() instanceof TimeoutException) {
						timeouts.incrementAndGet();
					} else {
						t.printStackTrace();
					}
				}
			});
		}
		cdl.await();
		long e1 = System.currentTimeMillis() - s1;
		System.out.println("一共耗时: " + e1 + " ms");
		System.out.println("超时异常数: " + timeouts);
		System.out.println("事务总数: " + count * 20);
		System.out.println("只读池req发送数: " + getWriteReqCounter(source.readPool()));
		System.out.println("只读池resp处理数: " + getPollRespCounter(source.readPool()));
		System.out.println("只读池resp等待数: " + getRespWaitingCount(source.readPool()));
		if (source.readPool() != source.writePool()) {
			System.out.println("可写池req发送数: " + getWriteReqCounter(source.writePool()));
			System.out.println("可写池resp处理数: " + getPollRespCounter(source.writePool()));
			System.out.println("可写池resp等待数: " + getRespWaitingCount(source.writePool()));
		}
		System.out.println("-----------------------------------------------------");

		s1 = System.currentTimeMillis();
		source.close();
		asyncGroup.close();
		e1 = System.currentTimeMillis() - s1;
		System.out.println("关闭过程: " + e1 + " ms");
		assert timeouts.get() == 0;
		System.out.println("\r\n\r\n");
	}

	protected static long getRespWaitingCount(PgClient client) {
		try {
			Method method = Client.class.getDeclaredMethod("getRespWaitingCount");
			method.setAccessible(true);
			return (Long) method.invoke(client);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected static int randomId() {
		return random.nextInt(10000) + 1;
	}

	protected static LongAdder getWriteReqCounter(PgClient client) {
		try {
			Field field = Client.class.getDeclaredField("reqWritedCounter");
			field.setAccessible(true);
			return (LongAdder) field.get(client);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected static LongAdder getPollRespCounter(PgClient client) {
		try {
			Field field = Client.class.getDeclaredField("respDoneCounter");
			field.setAccessible(true);
			return (LongAdder) field.get(client);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
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

	// @DistributeTable(strategy = Record.TableStrategy.class)
	@Entity
	public static class Record {

		public static class TableStrategy implements DistributeTableStrategy<Record> {

			private static final String format = "%1$tY%1$tm";

			@Override
			public String[] getTables(String table, FilterNode node) {
				int pos = table.indexOf('.');
				return new String[] {table.substring(pos + 1) + "_" + String.format(format, System.currentTimeMillis())
				};
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

		public Record() {}

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

		public Fortune() {}

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

		public static World[] modifyNumber(List<World> list) {
			for (World world : list) {
				world.setRandomNumber(world.getRandomNumber() + 2);
			}
			return list.toArray(new World[list.size()]);
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
