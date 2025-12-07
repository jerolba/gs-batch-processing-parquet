package com.example.batchprocessing;

import org.springframework.batch.infrastructure.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.jerolba.carpet.CarpetReader;
import com.jerolba.carpet.CloseableIterator;

public class ParquetItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> {

    private final CarpetReader<T> carpetReader;
    private CloseableIterator<T> iterator;

    public ParquetItemReader(CarpetReader<T> carpetReader) {
        setExecutionContextName(ClassUtils.getShortName(ParquetItemReader.class));
        this.carpetReader = carpetReader;
    }

    @Override
    protected void doOpen() throws Exception {
        this.iterator = carpetReader.iterator();
    }

    @Override
    protected T doRead() throws Exception {
        return iterator.hasNext() ? iterator.next() : null;
    }

    @Override
    protected void doClose() throws Exception {
        this.iterator.close();
    }

}