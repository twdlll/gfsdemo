package com.twd;

import com.twd.role.ChunkServer;
import com.twd.role.Client;
import com.twd.role.Master;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


/**
 * @author twd
 * @description
 * @date 2019-12-05
 */
public class Demo {
    public static void main(String[] args) {
        // configure
        Master master = new Master();
        Client client = new Client(master);
        List<ChunkServer> servers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            servers.add(new ChunkServer(String.valueOf(i), master));
        }

        // test create
        client.create("first.txt");
        client.create("second.txt");
        client.create("third.txt");
        client.create("fourth.txt");

        // master.printChunkServerInfos();

        // write test
        String writeData = "abcdefg,hijklmn";
        client.write("fourth.txt", 1800, ByteBuffer.wrap(writeData.getBytes(StandardCharsets.UTF_8)));

        // read test
        ByteBuffer buffer = client.read("fourth.txt", 1800, 5);
    }
}
