package com.twd.element;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author twd
 * @description
 * @date 2019-12-04
 */
public class Chunk {
    private int chunkHandle;

    private final String filename;

    private FileChannel fileChannel;

    private Logger logger;

    public int getChunkHandle() {
        return chunkHandle;
    }

    public void setChunkHandle(int chunkHandle) {
        this.chunkHandle = chunkHandle;
    }

    public String getFilename() {
        return filename;
    }

    public Chunk(int chunkHandle, String prefix) {
        this.chunkHandle = chunkHandle;
        filename = String.format("%s/%d.chunk", prefix, this.chunkHandle);
        logger = LoggerFactory.getLogger(filename);
        try {
            fileChannel = new RandomAccessFile(filename, "rw").getChannel();
        } catch (IOException e) {
            logger.error("open chunk error");
        }
    }

    private void checkFC() {
        if (null == fileChannel) {
            try {
                fileChannel = new RandomAccessFile(filename, "rw").getChannel();
            } catch (IOException e) {
                logger.error("open chunk error");
            }
        }
    }

    public ByteBuffer read(int offset, int len) {
        checkFC();
        ByteBuffer buffer = ByteBuffer.allocate(len);
        try {
            fileChannel.position(offset);
            fileChannel.read(buffer);
            logger.info("[{}:{}]-[read]-[{}]", filename, offset, new String(buffer.array(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            logger.error("read chunk error");
        }
        return buffer;
    }

    public boolean write(int offset, ByteBuffer byteBuffer) {
        checkFC();
        try {
            fileChannel.position(offset);
            fileChannel.write(byteBuffer);
            logger.info("[{}:{}]-[write]-[{}]", filename, offset, new String(byteBuffer.array(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            logger.error("write chunk error");
            return false;
        }
        return true;
    }

    public boolean close() {
        try {
            fileChannel.close();
            fileChannel = null;
        } catch (IOException e) {
            logger.error("close chunk error");
            return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Chunk)) {
            return false;
        }
        Chunk other = (Chunk) obj;
        return chunkHandle == other.chunkHandle;
    }

    @Override
    public int hashCode() {
        return chunkHandle;
    }
}
