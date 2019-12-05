package com.twd.element;

/**
 * @author twd
 * @description the chunk request which contains filename and chunk index
 *              the client get the chunk metadata by sending chunk request to master
 * @date 2019-12-04
 */
public class ChunkRequest {
    private String filename;

    private int chunkIndex;

    public String getFilename() {
        return filename;
    }

    public int getChunkIndex() {
        return chunkIndex;
    }

    public ChunkRequest(String filename, int chunkIndex) {
        this.filename = filename;
        this.chunkIndex = chunkIndex;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ChunkRequest)) {
            return false;
        }
        ChunkRequest other = (ChunkRequest) obj;
        return filename.equals(other.filename) && chunkIndex == other.chunkIndex;
    }

    @Override
    public int hashCode() {
        return filename.hashCode() * 100 + chunkIndex;
    }
}
