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

package cn.liubinbin.kdb.server.interf;

import cn.liubinbin.kdb.conf.KdbConfig;
import cn.liubinbin.kdb.grpc.KdbServiceGrpc;
import cn.liubinbin.kdb.grpc.KdbSqlRequest;
import cn.liubinbin.kdb.grpc.KdbSqlResponse;
import cn.liubinbin.kdb.server.executor.Executor;
import cn.liubinbin.kdb.server.parser.Parser;
import cn.liubinbin.kdb.server.planer.Plan;
import cn.liubinbin.kdb.server.planer.Planer;
import cn.liubinbin.kdb.server.store.StoreManage;
import cn.liubinbin.kdb.server.table.TableManage;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import org.apache.calcite.sql.SqlNode;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * @author liubinbin
 * @info Server that manages startup/shutdown of a KdbGrpcServer.
 */
public class KdbGrpcServer {

    private static final Logger logger = Logger.getLogger(KdbGrpcServer.class.getName());

    private Server server;
    private final TableManage tableManage;
    private final StoreManage storeManage;
    private final int kdbServerPort;

    public KdbGrpcServer(KdbConfig kdbConfig, TableManage tableManage, StoreManage storeManage) {
        this.tableManage = tableManage;
        this.storeManage = storeManage;
        this.kdbServerPort = kdbConfig.getKdbServerPort();
    }

    public void start() throws IOException {
        /* The port on which the server should run */
        server = Grpc.newServerBuilderForPort(kdbServerPort, InsecureServerCredentials.create())
                .addService(new SqlRequestImpl())
                .build()
                .start();
        logger.info("KDB Server started, listening on " + kdbServerPort);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down KDB gRPC server since JVM is shutting down");
                try {
                    KdbGrpcServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException, IOException {
        if (server != null) {
            server.awaitTermination();
        }
        tableManage.close();
        storeManage.close();
    }

    class SqlRequestImpl extends KdbServiceGrpc.KdbServiceImplBase {

        @Override
        public void sqlSingleRequest(KdbSqlRequest req, StreamObserver<KdbSqlResponse> responseObserver) {
            // request
            String sql = req.getSql();
            logger.info("KDB Server receive sql " + sql);
            System.out.println("sql : " + sql);
            // parser
            SqlNode sqlNode = Parser.parse(sql);
            // planer
            Plan plan = Planer.plan(sqlNode);
            // executor
            KdbSqlResponse response = new Executor(tableManage).execute(plan);
            // return
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
