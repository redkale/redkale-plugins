/*
 */

package org.redkalex.source.mongo;

import com.mongodb.MongoClientSettings;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModelBuilder;
import org.bson.codecs.pojo.Convention;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.json.JsonWriter;

/**
 *
 * @author zhangjx
 */
public class TestCodecProvider {
    public static void main(String[] args) throws Throwable {
        MongoClientSettings.Builder settingBuilder = MongoClientSettings.builder();
        CodecRegistry registry = CodecRegistries.fromRegistries(
                MongoClientSettings.getDefaultCodecRegistry(),
                CodecRegistries.fromProviders(PojoCodecProvider.builder()
                        .automatic(true)
                        .conventions(List.of(new MongoConvention()))
                        .build()));
        settingBuilder.codecRegistry(registry);
        MongoClientSettings setting = settingBuilder.build();
        CodecRegistry reg = setting.getCodecRegistry();
        /**
         * @see org.bson.codecs.pojo.AutomaticPojoCodec
         */
        Codec codec = reg.get(TestRecord.class);
        StringWriter pw = new StringWriter();
        JsonWriter writer = new JsonWriter(pw);
        TestRecord bean = new TestRecord();
        EncoderContext context = EncoderContext.builder().build();
        bean.setCreateTime(1);
        bean.setStatus((short)1);
        bean.setName("haha");
        bean.setRecordid("xxxx");
        codec.encode(writer, bean, context);
        System.out.println(pw);
    }

    public static class MongoConvention implements Convention {

        @Override
        public void apply(ClassModelBuilder<?> builder) {

            Set<String> cols = Set.of("recordid", "status");

            Set<String> fields = new HashSet<>();
            builder.getPropertyModelBuilders().forEach(p -> {
                fields.add(p.getWriteName());
            });
            for (String field : fields) {
                if (!cols.contains(field)) {
                    builder.removeProperty(field);
                }
            }
        }
    }
}
