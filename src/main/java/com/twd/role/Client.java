package com.twd.role;

import com.twd.element.ChunkMetadata;
import com.twd.element.ChunkRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author twd
 * @description GFS client, execute file operations through interaction with master and chunkserver
 * @date 2019-12-04
 */
public class Client {
    private Master master;
    /**
     * cache the chunk metadata
     */
    private Map<ChunkRequest, ChunkMetadata> chunkMetadataCache = new HashMap<>();

    private Logger logger = LoggerFactory.getLogger(Client.class);

    public Client(Master master) {
        this.master = master;
    }

    /**
     * compute chunk index with given offset
     * @param offset the read/write offset in bytes
     * @return chunk index
     */
    private int computeChunkIndex(int offset) {
        return offset / ChunkMetadata.CHUNK_SIZE ;
    }

    /**
     * get the metadata for given filename and offset
     * @param filename
     * @param offset
     * @return
     */
    private ChunkMetadata getChunkMetadata(String filename, int offset, int request) {
        int chunkIndex = computeChunkIndex(offset);
        ChunkRequest chunkRequest = new ChunkRequest(filename, chunkIndex);
        // first search in cache
        ChunkMetadata metadata = chunkMetadataCache.getOrDefault(chunkRequest, null);
        if (null == metadata) {
            // ask master for metadata
            metadata = master.getChunkMetadata(chunkRequest, request);
            if (null == metadata) {
                logger.info("get chunk's metadata error");
                return null;
            }
            // update cache
            chunkMetadataCache.put(chunkRequest, metadata);
        }
        return metadata;
    }

    /**
     * create file
     * @param filename
     * @return
     */
    public boolean create(String filename) {
        ChunkMetadata metadata = getChunkMetadata(filename, 0, Master.REQUEST_CREATE);
        if (null == metadata) {
            logger.info("create file {} error", filename);
            return false;
        }
        // the first is the primary
        ChunkServer[] servers = metadata.getChunkServers();
        boolean success = servers[0].create(metadata.getChunkHandle());
        if (success) {
            logger.info("create file {} success", filename);
        } else {
            logger.info("create file {} error", filename);
        }
        return success;
    }

    /**
     * @param filename
     * @param offset
     * @param len
     * @return
     */
    public ByteBuffer read(String filename, int offset, int len) {
        ChunkMetadata metadata = getChunkMetadata(filename, offset, Master.REQUEST_READ);
        // read don't need primary's participation, so select one to read
        int selectIndex = new Random().nextInt(Master.REPLICA_NUM);
        return metadata.getChunkServers()[selectIndex].read(metadata.getChunkHandle(), offset % ChunkMetadata.CHUNK_SIZE, len);
    }


    public boolean write(String filename, int offset, ByteBuffer buffer) {
        ChunkMetadata metadata = getChunkMetadata(filename, offset, Master.REQUEST_WRITE);
        // transform offset of file to offset of chunk
        offset = offset % ChunkMetadata.CHUNK_SIZE;
        ChunkServer[] servers = metadata.getChunkServers();
        // first, push data to these chunkserver
        logger.info("begin push data");
        for (ChunkServer server : servers) {
            if (!server.push(metadata.getChunkHandle(), offset, buffer)) {
                logger.error("push data error");
                return false;
            }
        }
        logger.info("push data success");
        // send write request to primary
        boolean finish = false;
        if ((finish = servers[0].write(metadata.getChunkHandle(), offset))) {
            logger.info("write data success");
        } else {
            logger.error("write data error");
        }
        return finish;
    }
}
