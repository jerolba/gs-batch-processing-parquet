package com.example.batchprocessing;

import java.io.File;
import java.io.IOException;

import javax.sql.DataSource;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.infrastructure.item.support.CompositeItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.jerolba.carpet.CarpetReader;
import com.jerolba.carpet.CarpetWriter;
import com.jerolba.carpet.io.FileSystemInputFile;
import com.jerolba.carpet.io.FileSystemOutputFile;

@Configuration
public class BatchConfiguration {

    // tag::readerwriterprocessor[]
    @Bean
    public ParquetItemReader<Person> reader() throws IOException {
        InputFile inputFile = new FileSystemInputFile(new File("/tmp/sample-data.parquet"));
        CarpetReader<Person> reader = new CarpetReader<>(inputFile, Person.class);
        return new ParquetItemReader<>(reader);
    }

    @Bean
    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Person> jdbcWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)")
                .dataSource(dataSource)
                .beanMapped()
                .build();
    }
    // end::readerwriterprocessor[]

    @Bean
    public ParquetItemWriter<Person> parquetWriter() throws IOException {
        OutputFile outputFile = new FileSystemOutputFile(new File("/tmp/processed-data.parquet"));
        CarpetWriter<Person> carpetWriter = new CarpetWriter<>(outputFile, Person.class);
        return new ParquetItemWriter<>(carpetWriter);
    }

    // tag::jobstep[]
    @Bean
    public Job importUserJob(JobRepository jobRepository, Step step1, JobCompletionNotificationListener listener) {
        return new JobBuilder(jobRepository)
                .listener(listener)
                .start(step1)
                .build();
    }

    @Bean
    public CompositeItemWriter<Person> compositeWriter(JdbcBatchItemWriter<Person> jdbcWriter,
            ParquetItemWriter<Person> parquetWriter) {
        return new CompositeItemWriter<>(jdbcWriter, parquetWriter);
    }

    @Bean
    public Step step1(JobRepository jobRepository, DataSourceTransactionManager transactionManager,
            ParquetItemReader<Person> reader, PersonItemProcessor processor, CompositeItemWriter<Person> writer) {
        return new StepBuilder(jobRepository)
                .<Person, Person>chunk(3)
                .transactionManager(transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }
    // end::jobstep[]
}
