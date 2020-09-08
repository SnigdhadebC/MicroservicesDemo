package com.stackroute.productservice.repository;

import com.stackroute.productservice.model.Product;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


@DataMongoTest
@ExtendWith(SpringExtension.class)
public class ProductRepositoryEmbeddedMongoDbTests {

    @Autowired
    private ProductRepository productRepository;

    @BeforeEach
    public void dataSetUp() {

        Product product1 = new Product("1", "Carrot", "Organic Carrot", "Vegetables", 5.5, "http://test.com/testurl1");
        Product product2 = new Product("2", "Chilli", "Organic Chilli", "Vegetables", 2.5, "http://test.com/testurl2");

        productRepository.save(product1);
        productRepository.save(product2);
    }

    @Test
    public void givenProductCategoryThenReturnListOfProductsFromDB() {
        List<Product> vegetables = productRepository.findByCategoryIgnoreCase("Vegetables");
        assertThat(vegetables)
                .isNotEmpty();
        assertThat(vegetables)
                .extracting(Product::getName)
                .contains("Carrot", "Chilli");
    }
}