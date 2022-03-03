package com.springdataCassandraNativeCompare;

import java.util.concurrent.Executor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@SpringBootApplication
@ComponentScan({"com.springdataCassandraNativeCompare*"})
@EntityScan({"com.springdataCassandraNativeCompare.*" })
@EnableCassandraRepositories("com.springdataCassandraNativeCompare.springData")
@EnableAsync
public class SpringDataCassandraNativeCompareApplication {



    public static void main(String[] args) {
//    	System.setProperty("datastax-java-driver.advanced.request-tracker.class", "RequestLogger");
//    	System.setProperty("datastax-java-driver.advanced.request-tracker.logs.success.enabled", "true");
//    	System.setProperty("datastax-java-driver.advanced.request-tracker.logs.error.enabled", "true");

    	//System.setProperty("datastax-java-driver.advanced.request-tracker.logs.slow.enabled", "true");
    	
    	SpringApplication.run(SpringDataCassandraNativeCompareApplication.class, args);


    }

    @Bean(name = "threadPoolExecutor")
    public Executor getAsyncExecutor() {
    	ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    	executor.setCorePoolSize(20);
    	executor.setMaxPoolSize(200);
    	executor.setQueueCapacity(400);
    	executor.setThreadNamePrefix("threadPoolExecutor-");
    	executor.initialize();
    	return executor;
    }





}
