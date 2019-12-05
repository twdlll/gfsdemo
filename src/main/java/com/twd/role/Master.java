package com.twd.role;

import com.twd.element.ChunkMetadata;
import com.twd.element.ChunkRequest;

import java.util.*;

/**
 * @author twd
 * @description be responsible for providing metadata to client
 * @date 2019-12-04
 */
public class Master {
    /**
     * add to queue
     */
    private static class ChunkServerInfo implements Comparable<ChunkServerInfo>{
        ChunkServer server;
        /**
         * chunk num of the chunk server
         */
        int chunkNum = 0;

        ChunkServerInfo(ChunkServer server, int chunkNum) {
            this.server = server;
            this.chunkNum = chunkNum;
        }

        @Override
        public int compareTo(ChunkServerInfo o) {
            return Integer.compare(chunkNum, o.chunkNum);
        }

        @Override
        public int hashCode() {
            return server.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof ChunkServerInfo)) {
                return false;
            }
            ChunkServerInfo other = (ChunkServerInfo)obj;
            return server == other.server;
        }
    }

    public static final int REQUEST_CREATE = 1;
    public static final int REQUEST_READ = 2;
    public static final int REQUEST_WRITE = 3;

    /**
     * store the chunk metadata: {chunk handle} -> {chunk metadata}
     */
    private Map<Integer, ChunkMetadata> chunkMetadataMap = new HashMap<>();

    /**
     * replication num
     */
    public static final int REPLICA_NUM = 3;

    /**
     * contains all chunkservers
     */
    private List<ChunkServer> chunkServers = new ArrayList<>();

    /**
     * select the chunkservers which have the smallest chunk num
     */
    private List<ChunkServerInfo> chunkServerInfos = new ArrayList<>();

    public Master() { }

    /**
     * add chunkserver
     * @param server
     */
    public void addChunkServers(ChunkServer server) {
        chunkServers.add(server);
    }

    /**
     * add chunkserver's info
     * @param server
     * @param chunkNum
     */
    public void addChunkServerInfo(ChunkServer server, int chunkNum) {
        ChunkServerInfo info = new ChunkServerInfo(server, chunkNum);
        chunkServerInfos.add(info);
    }

    /**
     * compute the hashcode of the combination of filename and chunk index
     * @param chunkRequest the combination of filename and chunk index
     * @return chunk handle
     */
    private int computeChunkHandle(ChunkRequest chunkRequest) {
        return chunkRequest.getFilename().hashCode() * 100 + chunkRequest.getChunkIndex();
    }

    /**
     * return chunk metadata for the chunk requested
     * @param chunkRequest
     * @return
     */
    public ChunkMetadata getChunkMetadata(ChunkRequest chunkRequest, int request) {
        int chunkHandle = computeChunkHandle(chunkRequest);
        ChunkMetadata metadata = chunkMetadataMap.getOrDefault(chunkHandle, null);
        if (null == metadata) {
            if (REQUEST_CREATE == request) {
                // create request
                metadata = createChunkMetadata(chunkRequest);
                metadata.getChunkServers()[0].becomePrimary(metadata);
            } else if (REQUEST_READ == request) {
                return null;
            } else {
                /**
                 * for write request, check if the file exists
                 */
                ChunkRequest fileRequest = new ChunkRequest(chunkRequest.getFilename(), 0);
                int fileChunkHandle = computeChunkHandle(fileRequest);
                if (chunkMetadataMap.containsKey(fileChunkHandle)) {
                    // the file exists, so create the corresponding chunk
                    metadata = createChunkMetadata(chunkRequest);
                    metadata.getChunkServers()[0].becomePrimary(metadata);
                    // create chunk
                    metadata.getChunkServers()[0].create(metadata.getChunkHandle());
                }
            }
        } else {
            if (REQUEST_CREATE == request) {
                // the file exists
                return null;
            } else {
                metadata.getChunkServers()[0].becomePrimary(metadata);
            }
        }
        return metadata;
    }

    /**
     * create metadata for new chunk, select least used chunkserver
     * the created metadata is temp, not add to chunk metadata map
     * @param chunkRequest
     * @return
     */
    private ChunkMetadata createChunkMetadata(ChunkRequest chunkRequest) {
        int chunkHandle = computeChunkHandle(chunkRequest);
        // select chunkservers
        ChunkServer[] servers = new ChunkServer[REPLICA_NUM];
        Collections.sort(chunkServerInfos);
        for (int i = 0; i < REPLICA_NUM; i++) {
            servers[i] = chunkServerInfos.get(i).server;
        }
        ChunkMetadata metadata = new ChunkMetadata(chunkHandle, servers);
        return metadata;
    }

    /**
     * when primary finish the create work, then primary will call this function
     * @param metadata
     */
    public void addChunkMetatdat(ChunkMetadata metadata) {
        chunkMetadataMap.put(metadata.getChunkHandle(), metadata);
        // update chunkserver info
        Set<ChunkServer> servers = new HashSet<>(Arrays.asList(metadata.getChunkServers()));
        for (ChunkServerInfo info : chunkServerInfos) {
            if (servers.contains(info.server)) {
                info.chunkNum++;
            }
        }
    }

    /**
     * when chunkserver start, it will check all chunks
     * then report the chunk metadata
     * @param chunkHandle
     * @param server
     */
    public void reportChunkMetadata(int chunkHandle, ChunkServer server) {
        ChunkMetadata metadata = chunkMetadataMap.getOrDefault(chunkHandle, null);
        if (null == metadata) {
            // update chunk metadata map
            metadata = new ChunkMetadata(chunkHandle, new ChunkServer[REPLICA_NUM]);
            chunkMetadataMap.put(chunkHandle, metadata);
        }
        ChunkServer[] servers = metadata.getChunkServers();
        for (int i = 0; i < REPLICA_NUM; i++) {
            if (null == servers[i]) {
                servers[i] = server;
                break;
            }
        }
    }

    public void printChunkServerInfos() {
        System.out.println("chunserver : chunkname");
        for (ChunkServerInfo info : chunkServerInfos) {
            System.out.println(info.server.getChunkServerName() + " : " + info.chunkNum);
        }
    }
}
