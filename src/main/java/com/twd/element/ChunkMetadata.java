package com.twd.element;

import com.twd.role.ChunkServer;

/**
 * @author twd
 * @description the chunk in GFS
 * @date 2019-12-04
 */
public class ChunkMetadata {
    /**
     * set chunk size to 1024KB
     */
    public static final int CHUNK_SIZE = 1 << 10;

    /**
     * use int to represent chunk handle
     */
    private int chunkHandle;

    /**
     * chunkservers which have the chunk
     */
    private ChunkServer[] chunkServers;

    public ChunkMetadata(int chunkHandle, ChunkServer[] chunkServers) {
        this.chunkHandle = chunkHandle;
        this.chunkServers = chunkServers;
    }

    public int getChunkHandle() {
        return chunkHandle;
    }

    public void setChunkHandle(int chunkHandle) {
        this.chunkHandle = chunkHandle;
    }

    public ChunkServer[] getChunkServers() {
        return chunkServers;
    }

    public void setChunkServers(ChunkServer[] chunkServers) {
        this.chunkServers = chunkServers;
    }
}
