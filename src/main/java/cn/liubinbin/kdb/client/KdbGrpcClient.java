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
import cn.liubinbin.kdb.utils.Contants;
import cn.liubinbin.kdb.utils.StringUtils;
import com.google.protobuf.ProtocolStringList;
import io.grpc.*;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

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
    public KdbSqlResponse sendSql(String sql) {
//        logger.info("Will try to send sql " + sql + " ...");
        KdbSqlRequest request = KdbSqlRequest.newBuilder().setSql(sql).build();
        KdbSqlResponse response;
        try {
            response = blockingStub.sqlSingleRequest(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return null;
        }
        return response;
    }

    private static void printRow(ProtocolStringList row) {
        // 格式化输出
        StringBuilder rowStr = new StringBuilder();
        rowStr.append(Contants.ROW_PRINT_SEPARATOR);
        for (String s : row) {
            rowStr.append(StringUtils.rightPaddingToFixLen(s, 10)).append(Contants.ROW_PRINT_SEPARATOR);
        }
        System.out.println(rowStr);
    }

    private static void printSeparator(int columnCount) {
        // 输出分割线
        StringBuilder separator = new StringBuilder();
        for (int i = 0; i < columnCount; i++) {
            separator.append("+");
            separator.append(StringUtils.repeat("-", 10)); // 假设每列宽度都是10
        }
        separator.append("+");
        System.out.println(separator);
    }

    private static void printResponse(KdbSqlResponse response) {
        if (response == null) {
            System.out.println("No response received.");
            return;
        }
        printSeparator(response.getHeader().getHeaderCount());
        printRow(response.getHeader().getHeaderList());
        printSeparator(response.getHeader().getHeaderCount());
        for (cn.liubinbin.kdb.grpc.Row row : response.getRowList()) {
            printRow(row.getValueList());
        }

        printSeparator(response.getHeader().getHeaderCount());
    }

    /**
     * Communicate with server.
     */
    public static void main(String[] args) throws Exception {
        String kdbServerAddr = "localhost:50501";
//        String sql = "describe database kdb";
//        String sql = "describe table stu";
//        String sql = "create table stu(id int, name varchar(256), age int)"; // id为主键
//        String sql = "insert into stu (id, name, age) VALUES (1, 'Alice', 29)";
//        String sql = "select * from stu";
//        String sql = "select * from stu where id = 1";
//        String sql = "select * from stu where id = 1 and name = 'haha'";
//        String sql = "select * from stu order by age limit 10";
//        String sql = "select id,name from stu";

        ManagedChannel channel = Grpc.newChannelBuilder(kdbServerAddr, InsecureChannelCredentials.create())
                .build();
        try {
            KdbGrpcClient client = new KdbGrpcClient(channel);

            try (Terminal terminal = TerminalBuilder.builder().build()) {
                LineReader lineReader = LineReaderBuilder.builder()
                        .terminal(terminal)
                        .build();
                lineReader.variable(LineReader.HISTORY_FILE, "/tmp/history")
                        .variable(LineReader.HISTORY_FILE_SIZE, 1_000);

                String input = null;
                System.out.println("Enter sql to query or 'exit' to quit: ");
                while (true) {
                    input = lineReader.readLine("> ");

                    if (input.isEmpty()) {
                        continue;
                    }

                    if (Contants.EXIT.equalsIgnoreCase(input)) {
                        System.out.println("Exiting...");
                        break;
                    } else {
                        System.out.println("You entered: " + input);
                        KdbSqlResponse kdbSqlResponse = client.sendSql(input);
                        printResponse(kdbSqlResponse);
                    }
                }
            }



//            Scanner scanner = new Scanner(System.in);
//            System.out.println("Enter sql to query or 'exit' to quit: ");
//
//            while (true) {
//                System.out.print("> ");
//                String input = scanner.nextLine().trim();
//
//                if (input.isEmpty()) {
//                    continue;
//                }
//
//                if (Contants.EXIT.equalsIgnoreCase(input)) {
//                    System.out.println("Exiting...");
//                    break;
//                } else {
//                    System.out.println("You entered: " + input);
//                    KdbSqlResponse kdbSqlResponse = client.sendSql(input);
//                    printResponse(kdbSqlResponse);
//                }
//            }

//            scanner.close();
        } finally {
            // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
            // resources the channel should be shut down when it will no longer be used. If it may be used
            // again leave it running.
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
