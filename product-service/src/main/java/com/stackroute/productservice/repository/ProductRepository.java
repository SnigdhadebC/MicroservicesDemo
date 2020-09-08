package com.stackroute.productservice.repository;

import com.stackroute.productservice.model.Product;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Repository used for storing product details in MongoDb
 */
@Repository
public interface ProductRepository extends MongoRepository<Product, String> {

    List<Product> findByCategoryIgnoreCase(String category);

}
