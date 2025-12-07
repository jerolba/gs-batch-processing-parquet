package com.example.batchprocessing;

import java.io.IOException;

import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemStreamException;
import org.springframework.batch.infrastructure.item.support.AbstractItemStreamItemWriter;

import com.jerolba.carpet.CarpetWriter;

public class ParquetItemWriter<T> extends AbstractItemStreamItemWriter<T> {

    private final CarpetWriter<T> carpetWriter;

    public ParquetItemWriter(CarpetWriter<T> carpetWriter) {
        this.carpetWriter = carpetWriter;
    }

    @Override
    public void write(Chunk<? extends T> chunk) throws Exception {
        for (T item : chunk.getItems()) {
            carpetWriter.write(item);
        }
    }

    @Override
    public void close() {
        try {
            carpetWriter.close();
        } catch (IOException e) {
            throw new ItemStreamException(e.getMessage(), e);
        }
    }

}