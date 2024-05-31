package com.kkkoke.txmanager;

import com.kkkoke.txmanager.netty.NettyServer;
import lombok.extern.slf4j.Slf4j;

/**
 * @author KeyCheung
 * @date 2023/10/28
 * @desc
 */
@Slf4j
public class TxManagerMain {
    public static void main(String[] args) {
        NettyServer nettyServer = new NettyServer();
        nettyServer.start("localhost", 8080);
        log.info("TxManager服务启动");
    }
}
