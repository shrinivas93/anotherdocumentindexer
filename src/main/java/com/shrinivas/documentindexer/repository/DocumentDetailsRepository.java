package com.shrinivas.documentindexer.repository;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.shrinivas.documentindexer.document.DocumentDetails;

public interface DocumentDetailsRepository extends MongoRepository<DocumentDetails, String> {

}
