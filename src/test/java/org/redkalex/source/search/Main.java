/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.search;

import org.redkale.boot.LoggingFileHandler;
import org.redkale.source.*;
import org.redkale.source.Range.LongRange;
import org.redkale.util.*;
import static org.redkale.source.AbstractDataSource.*;
import static org.redkale.source.SearchQuery.SEARCH_FILTER_NAME;

/**
 *
 * @author zhangjx
 */
public class Main {

    public static void main(String[] args) throws Throwable {
        {
            FilterNode node = FilterNode.create("power", 7).and(FilterNode.create("type", "words").or("createTime", 1621581722242L));
            System.out.println(node.getExpress() + "========" + node.toString());
            System.out.println(new SearchRequest().filterNode(null, node));
            node = FilterNode.create("power", 7).and("power2", 6).or(FilterNode.create("type", "words").and("createTime", 1621581722242L));
            System.out.println(node.getExpress() + "========" + node.toString());
            System.out.println(new SearchRequest().filterNode(null, node));
        }
        LoggingFileHandler.initDebugLogConfig();
        OpenSearchSource source = new OpenSearchSource();
        source.init(AnyValue.create().addValue(DATA_SOURCE_URL, "http://127.0.0.1:9200"));
        final boolean insert = false;
        if (insert) {
            System.out.println("删除表的结果是否成功:" + (source.dropTable(TestPost.class) == 1));
            Thread.sleep(100);
            //
            TestPost post = new TestPost();
            post.setType("forum");
            post.setUserid(16564 + (int) (System.currentTimeMillis() % 1000));
            post.setCreateTime(System.currentTimeMillis());
            post.setPower(2);
            post.setPostid(TestPost.createId(post.getType(), post.getUserid(), post.getCreateTime()));
            post.setTitle("这是个标题内容");
            post.setPubContent("这是一个论坛aaaaa内<b>容</b> Hello World！");

            TestPost post2 = new TestPost();
            post2.setType("words");
            post2.setUserid(245464 + (int) (System.currentTimeMillis() % 1000));
            post2.setCreateTime(System.currentTimeMillis() + 100);
            post2.setPostid(TestPost.createId(post2.getType(), post2.getUserid(), post2.getCreateTime()));
            post2.setTitle("这是个标题内容");
            post2.setPubContent("这是一个论坛bbbbb内<b>容</b> Hello World！");

            TestPost post3 = new TestPost();
            post3.setType("words");
            post3.setUserid(345464 + (int) (System.currentTimeMillis() % 1000));
            post3.setCreateTime(System.currentTimeMillis() + 200);
            post3.setPower(3);
            post3.setPostid(TestPost.createId(post3.getType(), post3.getUserid(), post3.getCreateTime()));
            post3.setTitle("这是个标题内容");
            post3.setPubContent("这是一个论坛ccccc内<b>容</b> Hello World！");

            TestPost post4 = new TestPost();
            post4.setType("words");
            post4.setUserid(445464 + (int) (System.currentTimeMillis() % 1000));
            post4.setCreateTime(System.currentTimeMillis() + 300);
            post4.setPostid(TestPost.createId(post4.getType(), post4.getUserid(), post4.getCreateTime()));
            post4.setTitle("这是个标题内容");
            post4.setPubContent("这是一个论坛ddddd内容 Hello World！");
            source.insert(post, post2, post3, post4);
            source.updateMapping(TestPost.class);

            long now = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                post4 = new TestPost();
                post4.setType("words");
                post4.setUserid(445464 + (int) (System.currentTimeMillis() % 1000));
                post4.setCreateTime(now + 400 + i);
                post4.setPostid(TestPost.createId(post4.getType(), post4.getUserid(), post4.getCreateTime()));
                post4.setTitle("");
                post4.setPubContent("这是一个论坛" + i + i + i + i + i + i + i + "内容 Hello World！");
                source.insert(post4);
            }

            for (int i = 0; i < 100; i++) {
                TestComment comment = new TestComment();
                comment.setUserid(post4.getUserid());
                comment.setCreateTime(now + 500 + i);
                comment.setCommentid(comment.getUserid() + "-" + comment.getCreateTime());
                comment.setTitle("这是一个评论的标题");
                comment.setContent("这是一个评论的内容");
                source.insert(comment);
            }
            //source.updateMapping(TestPost.class);
            Thread.sleep(1000);
            System.out.println("第1个find结果: " + source.find(TestPost.class, "createTime", post.getCreateTime()));
            System.out.println("第2个find结果: " + source.find(TestPost.class, SelectColumn.excludes("pubContent"), post2.getPostid()));
            System.out.println("第3个find结果: " + source.find(TestPost.class, SelectColumn.includes("pubContent"), post3.getPostid()));
            //System.out.println("第4个delete结果: " + source.delete(TestPost.class, post4.getPostid()));
            System.out.println(source.queryColumnList("power", TestPost.class, FilterNode.create("createTime", new LongRange(post3.getCreateTime() + 1, -1L))));
        }
        //System.out.println(FilterNode.create("power", new int[]{1, 3}).and("createTime", 1621743242363L));
        SearchQuery.SearchSimpleQuery searchBean = SearchQuery.create("dddd内容", "title", "content", "pubContent");
        SearchQuery.SearchSimpleHighlight highlightBean = SearchQuery.createHighlight();
        highlightBean.tag("<font color=red>", "</font>");
        searchBean.setHighlight(highlightBean);
        System.out.println(source.queryList(TestPost.class, new Flipper(5, 5, "createTime DESC"), FilterNode.create(SEARCH_FILTER_NAME, searchBean).and("createTime", FilterExpress.GREATERTHAN, 1L)));
        //System.out.println(source.queryColumnSet("power", TestPost.class, FilterNode.create("power", new int[]{1, 3}).and("createTime", 1621581722242L)));
        //System.out.println(source.queryColumnMap(TestPost.class, "power", FilterFunc.DISTINCTCOUNT, null));
        //System.out.println(source.getNumberResult(TestPost.class, FilterFunc.MIN, 5, "power", (FilterNode) null));
        //System.out.println(source.delete(TestPost.class, "fqo4-kos8mcmd", "fqo4-koscfh48"));
        //System.out.println(source.updateColumn(TestPost.class, "w7ek8-koscfh4i", ColumnValue.inc("power2", 3), ColumnValue.mov("payCount2", 11)));
//        String json = "{\"_index\":\"platf\",\"_type\":\"postrecord\",\"_id\":\"w7ek8-koscfh4i\",\"_version\":3,\"_seq_no\":8,\"_primary_term\":1,\"found\":true,\"_source\":{\"commentCount\":0,\"commentFlag\":10,\"createTime\":1621239988338,\"freeImgCount\":0,\"hideEscapedFlag\":0,\"imgCount\":0,\"lastCommentTime\":0,\"lastCommentUserid\":0,\"likeCount\":0,\"niceFlag\":10,\"payCount\":11,\"postid\":\"w7ek8-koscfh4i\",\"power\":7,\"pubContent\":\"这是一个论坛2222内容 Hello World！\",\"pubEscapedFlag\":0,\"pwdCount\":0,\"rewardCoin\":0,\"rewardFlag\":10,\"seeCoin\":0,\"shareCount\":0,\"status\":0,\"topFlag\":10,\"type\":\"words\",\"userid\":345464,\"visitCount\":0}}";
//        System.out.println(JsonConvert.root().convertFrom(new TypeToken<FindResult<PostRecord>>(){}.getType(), json).filterNodeBool()); 
    }

}
