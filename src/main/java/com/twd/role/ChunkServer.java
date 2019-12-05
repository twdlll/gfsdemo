package com.twd.role;

import com.twd.element.Chunk;
import com.twd.element.ChunkMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author twd
 * @description store chunk
 * @date 2019-12-04
 */
public class ChunkServer {
    /**
     * the root directory to store chunks
     */
    private String rootFolder;

    public String getChunkServerName() {
        return chunkServerName;
    }

    /**
     * the server's name
     */
    private String chunkServerName;

    /**
     * store all stored chunks: {chunk handle} -> {chunk}
     */
    private Map<Integer, Chunk> chunks;

    /**
     * the chunkserver is the primary for these chunk handles : {chunk handle} -> {chunk}
     */
    private Map<Integer, ChunkMetadata> primaries;

    private Master master;

    private static class BufferedData {
        int chunkHandle;
        int offset;
        ByteBuffer buffer;
        BufferedData(int chunkHandle, int offset, ByteBuffer buffer) {
            this.chunkHandle = chunkHandle;
            this.offset = offset;
            this.buffer = buffer;
        }
    }
    /**
     * buffer the pushed data
     */
    private List<BufferedData> bufferedDatas = new ArrayList<>();

    private Logger logger;

    public ChunkServer(String rootFolder, Master master) {
        this.master = master;
        this.master.addChunkServers(this);
        this.rootFolder = rootFolder;
        chunks = new HashMap<>();
        primaries = new HashMap<>();
        chunkServerName = String.format("chunkserver-%s", rootFolder);
        logger = LoggerFactory.getLogger(chunkServerName);
        // create root directory
        File rootDir = new File(rootFolder);
        if (rootDir.exists()) {
            // check all chunk, and report metadata to master
            checkChunks();
        } else {
            if (!rootDir.mkdir()) {
                logger.info("create root folder error");
            }
        }
        master.addChunkServerInfo(this, chunks.size());
    }

    /**
     * check chunk info
     */
    private void checkChunks() {
        File rootDir = new File(rootFolder);
        File[] files = rootDir.listFiles();
        if (null == files) {
            return ;
        }
        for (File file : files) {
            if (file.isFile()) {
                String filename = file.getName();
                int chunkHandle = Integer.parseInt(filename.substring(0, filename.lastIndexOf('.')));
                chunks.put(chunkHandle, new Chunk(chunkHandle, rootFolder));
                // report to master
                master.reportChunkMetadata(chunkHandle, this);
            }
        }
    }

    /**
     * create chunk for given chunk handle
     * @param chunkHandle
     * @return
     */
    public boolean create(int chunkHandle) {
        // check if the chunk exists
        if (!chunks.containsKey(chunkHandle)) {
            Chunk chunk = new Chunk(chunkHandle, rootFolder);
            chunks.put(chunkHandle, chunk);
        }
        if (primaries.containsKey(chunkHandle)) {
            // it's primary
            ChunkMetadata metadata = primaries.get(chunkHandle);
            for (ChunkServer server : metadata.getChunkServers()) {
                /**
                 * send the secondary for create request
                 * if one failed, the create function return false
                 */
                if (this != server && !server.create(chunkHandle)) {
                    return false;
                }
            }
            // report new chunk's metadata to master
            master.addChunkMetatdat(metadata);
            logger.info("primary create new chunk({}.chunk)", chunkHandle);
            return true;
        }
        logger.info("secondary create new chunk({}.chunk)", chunkHandle);
        return true;
    }

    /**
     * return the chunk num of this chunkserver
     * @return
     */
    public int getChunkNum() {
        return chunks.size();
    }

    /**
     * client send read request to replica, don't need primary's participation
     * @param chunkHandle
     * @param offset
     * @param len
     * @return
     */
    public ByteBuffer read(int chunkHandle, int offset, int len) {
        if (!chunks.containsKey(chunkHandle)) {
            logger.error("the file does not exist");
            return null;
        }
        Chunk chunk = chunks.get(chunkHandle);
        return chunk.read(offset, len);
    }

    /**
     * before send write request, client push the data to chunkserver
     * chunkservers buffer data after receiving
     * @param chunkHandle
     * @param offset
     * @param buffer
     * @return
     */
    public boolean push(int chunkHandle, int offset, ByteBuffer buffer) {
        BufferedData bufferedData = new BufferedData(chunkHandle, offset, buffer);
        if (!bufferedDatas.contains(bufferedData)) {
            bufferedDatas.add(bufferedData);
            logger.info("receive pushed data success");
        }
        return true;
    }

    /**
     * according to the write request, chunkserver find the buffered data
     * @param chunkHandle
     * @param offset
     * @return
     */
    private BufferedData getBufferedData(int chunkHandle, int offset) {
        BufferedData retData = null;
        for (BufferedData data : bufferedDatas) {
            // search data in buffer
            if (chunkHandle == data.chunkHandle && offset == data.offset) {
                retData = data;
                break;
            }
        }
        if (null != retData) {
            // remove data in buffer
            bufferedDatas.remove(retData);
        }
        return retData;
    }

    /**
     *
     * @param chunkHandle
     * @param offset
     * @return
     */
    public boolean write(int chunkHandle, int offset) {
        BufferedData bufferedData = getBufferedData(chunkHandle, offset);
        if (null == bufferedData) {
            logger.error("can't find buffered data ({}:{})", chunkHandle, offset);
            return false;
        }
        Chunk chunk = chunks.get(chunkHandle);
        // need to rewind
        bufferedData.buffer.rewind();
        if (!chunk.write(offset, bufferedData.buffer)) {
            // failure
            logger.error("{}'s {} write data error",
                    chunkHandle,
                    primaries.containsKey(chunkHandle) ? "primary" : "secondary");
            return false;
        }
        if (primaries.containsKey(chunkHandle)) {
            // send write request to other secondary
            ChunkMetadata metadata = primaries.get(chunkHandle);
            ChunkServer[] servers = metadata.getChunkServers();
            for (int i = 1; i < Master.REPLICA_NUM; i++) {
                if (!servers[i].write(chunkHandle, offset)) {
                    logger.error("secondary write error");
                    return false;
                }
            }
            logger.info("primary write success");
            return true;
        }
        logger.info("secondary write success");
        return true;
    }

    /**
     * become the primary of the specific chunk
     * @param metadata
     * @return
     */
    public void becomePrimary(ChunkMetadata metadata) {
        primaries.put(metadata.getChunkHandle(), metadata);
    }
}
