package com.kkkoke.txmanager.netty;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.kkkoke.txmanager.constant.CommandType;
import com.kkkoke.txmanager.constant.TransactionProperty;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author KeyCheung
 * @date 2023/10/28
 * @desc 作为事务管理者，它需要：
 * 1. 创建并保存事务组
 * 2. 保存各个子事务在对应的事务组内
 * 3. 统计并判断事务组内的各个子事务状态，以算出当前事务组的状态（提交or回滚）
 * 4. 通知各个子事务提交或回滚
 */
@Slf4j
public class NettyServerHandler extends ChannelInboundHandlerAdapter {

    private static ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    // 事务组中的事务状态列表
    private static final Map<String, List<String>> transactionStatusMap = new HashMap<>();

    // 事务组是否已经接收到结束的标记
    private static final Map<String, Boolean> isEndMap = new HashMap<>();

    // 事务组中应该有的事务个数
    private static final Map<String, Integer> transactionCountMap = new HashMap<>();

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        channelGroup.add(ctx.channel());
    }

    /**
     *
     * {groupId:List<子事务>}
     * 1. 接收创建事务组事件
     * 2. 接收子事务的注册事件
     * 3. 判断事务组的状态，如果该事务组中有一个事务需要回滚，那么整个事务组就需要回滚，反之，则整个事务组提交
     * 4. 通知所有客户端进行提交或回滚
     * @param ctx
     * @param msg
     * @throws Exception
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        log.info("接受数据：{}", msg.toString());

        JSONObject jsonObject = JSON.parseObject((String) msg);

        String command = jsonObject.getString(TransactionProperty.COMMAND); // create-创建事务，register-注册分支事务
        String groupId = jsonObject.getString(TransactionProperty.GROUP_ID); // 事务组id
        String transactionStatus = jsonObject.getString(TransactionProperty.TRANSACTION_STATUS); // 分支事务类型，commit-提交，rollback-回滚
        Integer transactionCount = jsonObject.getInteger(TransactionProperty.TRANSACTION_COUNT); // 分支事务数量
        Boolean isEnd = jsonObject.getBoolean(TransactionProperty.IS_END); // 是否是最后一个分支事务

        if (CommandType.CREATE.equals(command)) {
            // 创建事务组
            transactionStatusMap.put(groupId, new ArrayList<>());
        } else if (CommandType.REGISTER.equals(command)) {
            // 加入事务组
            transactionStatusMap.get(groupId).add(transactionStatus);

            if (isEnd) {
                isEndMap.put(groupId, true);
                transactionCountMap.put(groupId, transactionCount);
            }

            JSONObject result = new JSONObject();
            result.put(TransactionProperty.GROUP_ID, groupId);

            // 如果已经接收到结束事务的标记，比较事务是否已经全部到达，如果已经全部到达则看是否需要回滚
            if (isEndMap.get(groupId) && transactionCountMap.get(groupId).equals(transactionStatusMap.get(groupId).size())) {
                if (transactionStatusMap.get(groupId).contains(CommandType.ROLLBACK)) {
                    result.put(TransactionProperty.COMMAND, CommandType.ROLLBACK);
                    sendResult(result);
                } else {
                    result.put(TransactionProperty.COMMAND, CommandType.COMMIT);
                    sendResult(result);
                }
            }
        }
    }

    private void sendResult(JSONObject result) {
        for (Channel channel : channelGroup) {
            log.info("发送数据：{}", result.toJSONString());
            channel.writeAndFlush(result.toJSONString());
        }
    }
}
