/*
 * Copyright 2015 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.liubinbin.kdb.client;

import cn.liubinbin.kdb.grpc.KdbServiceGrpc;
import cn.liubinbin.kdb.grpc.KdbSqlRequest;
import cn.liubinbin.kdb.grpc.KdbSqlResponse;
import io.grpc.*;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author liubinbin
 * @date 2024/8/14
 * @info A simple client that requests a sql from the {@link cn.liubinbin.kdb.server.interf.KdbGrpcServer}.
 */
public class KdbGrpcClient {
    private static final Logger logger = Logger.getLogger(KdbGrpcClient.class.getName());

    private final KdbServiceGrpc.KdbServiceBlockingStub blockingStub;

    /**
     * Construct client for accessing HelloWorld server using the existing channel.
     */
    public KdbGrpcClient(Channel channel) {
        // 'channel' here is a Channel, not a ManagedChannel, so it is not this code's responsibility to
        // shut it down.

        // Passing Channels to code makes code easier to test and makes it easier to reuse Channels.
        blockingStub = KdbServiceGrpc.newBlockingStub(channel);
    }

    /**
     * Say hello to server.
     */
    public void sendSql(String sql) {
        logger.info("Will try to send sql " + sql + " ...");
        KdbSqlRequest request = KdbSqlRequest.newBuilder().setSql(sql).build();
        KdbSqlResponse response;
        try {
            response = blockingStub.sqlSingleRequest(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("response header: " + response.getHeader());
        logger.info("response data: " + response.getRowList());
    }

    /**
     * Communicate with server.
     */
    public static void main(String[] args) throws Exception {
        String kdbServerAddr = "localhost:50501";
//        String sql = "describe database kdb";
        String sql = "describe table test1";
//        String sql = "create table test3(id int, b int, c varchar(256))"; // id为主键
//        String sql = "insert into a (id, name) VALUES (1, 'Alice')";
//        String sql = "select * from a";
//        String sql = "select * from a where b = 1";
//        String sql = "select * from a where b = 1 and c = 'haha'";
//        String sql = "select * from a order by b limit 10";
//        String sql = "select a,b from c";

        ManagedChannel channel = Grpc.newChannelBuilder(kdbServerAddr, InsecureChannelCredentials.create())
                .build();
        try {
            KdbGrpcClient client = new KdbGrpcClient(channel);
            client.sendSql(sql);
        } finally {
            // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
            // resources the channel should be shut down when it will no longer be used. If it may be used
            // again leave it running.
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
