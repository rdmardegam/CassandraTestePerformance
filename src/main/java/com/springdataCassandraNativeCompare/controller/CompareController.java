package com.springdataCassandraNativeCompare.controller;


import com.springdataCassandraNativeCompare.springData.SpringDataRepository;
import com.springdataCassandraNativeCompare.springData.entity.DummyItem;

import lombok.extern.log4j.Log4j2;

import com.springdataCassandraNativeCompare.cassandraNative.CassandraNativeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.LongStream;


/**
 * @author Fatih Yıldızlı
 */

@RestController
@Log4j2
public class CompareController {

    @Autowired
    SpringDataRepository springDataRepository;

    @Autowired
    CassandraNativeRepository cassandraNativeRepository;


    @CrossOrigin(origins = {"*"})
    @RequestMapping(path = "/select/springdata", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<?> selectSpringData() {
        long startTime = System.currentTimeMillis();
        List<DummyItem> response = springDataRepository.selectAll();
        long finishTime = System.currentTimeMillis();
        System.out.println("Elapsed:" + (finishTime - startTime) + "ms");
        
        /*return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms",
                HttpStatus.OK);*/
        return new ResponseEntity<>(response,
                HttpStatus.OK);
    }

    @CrossOrigin(origins = {"*"})
    @RequestMapping(path = "/select/cassandraNative", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<?> selectCassandraNative() {
        long startTime = System.currentTimeMillis();
        
        //List<DummyItem> response = new ArrayList<DummyItem>();
        List<DummyItem> response =  Collections.synchronizedList(new ArrayList<DummyItem>());
        
        //response.addAll(cassandraNativeRepository.selectAll());
        //response.addAll(cassandraNativeRepository.selectAll());
        //response.addAll(cassandraNativeRepository.selectAll());
        
        // Faz 3 chaamdas em paralelo
        LongStream.range(0,3).parallel().forEach(v -> {
        	System.out.println(v);
        	response.addAll(cassandraNativeRepository.selectAll());
        });
        
        
        long finishTime = System.currentTimeMillis();
        //System.out.println();
        
        log.info("Elapsed:" + (finishTime - startTime) + "ms");
        
        /*return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms",
                HttpStatus.OK);*/
        return new ResponseEntity<>(response.size(),
                HttpStatus.OK);
        
    }

    @CrossOrigin(origins = {"*"})
    @RequestMapping(path = "/insert/springdata/{count}", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<?> insertSpringData(@PathVariable("count") long count) {
        long startTime = System.currentTimeMillis();
        LongStream.range(0,count).forEach(i-> springDataRepository.insert(i, "yıldızlı", "fatih"));
        long finishTime = System.currentTimeMillis();
        return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms",
                HttpStatus.OK);
    }

    @CrossOrigin(origins = {"*"})
    @RequestMapping(path = "/insert/cassandraNative/{count}", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<?> insertCassandraNative(@PathVariable("count") long count) {

        long startTime = System.currentTimeMillis();
        LongStream.range(0,count).forEach(i-> cassandraNativeRepository.insertAll(i));
        long finishTime = System.currentTimeMillis();
        return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms",
                HttpStatus.OK);
    }


    @CrossOrigin(origins = {"*"})
    @RequestMapping(path = "/update/springdata/{count}", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<?> updateSpringData(@PathVariable("count") long count) {

        long startTime = System.currentTimeMillis();
        LongStream.range(0,count).forEach(i-> springDataRepository.update(i, "FYildizli", "fatih"));
        long finishTime = System.currentTimeMillis();
        return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms",
                HttpStatus.OK);
    }

    @CrossOrigin(origins = {"*"})
    @RequestMapping(path = "/update/cassandraNative/{count}", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<?> updateCassandraNative(@PathVariable("count") long count) {

        long startTime = System.currentTimeMillis();
        LongStream.range(0,count).forEach(i-> cassandraNativeRepository.updateAll(i));
        long finishTime = System.currentTimeMillis();
        return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms",
                HttpStatus.OK);
    }

    @CrossOrigin(origins = {"*"})
    @RequestMapping(path = "/delete/springdata/{count}", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<?> deleteSpringData(@PathVariable("count") long count) {

        long startTime = System.currentTimeMillis();
        LongStream.range(0,count).forEach(i-> springDataRepository.delete(i));
        long finishTime = System.currentTimeMillis();
        return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms",
                HttpStatus.OK);
    }

    @CrossOrigin(origins = {"*"})
    @RequestMapping(path = "/delete/cassandraNative/{count}", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<?> deleteCassandraNative(@PathVariable("count") long count) {

        long startTime = System.currentTimeMillis();
        LongStream.range(0,count).forEach(i-> cassandraNativeRepository.deleteAll(i));
        long finishTime = System.currentTimeMillis();
        return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms",
                HttpStatus.OK);
    }

}