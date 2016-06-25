package com.shrinivas.documentindexer.repository;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.shrinivas.documentindexer.document.Index;

public interface IndexRepository extends MongoRepository<Index, String> {

}
