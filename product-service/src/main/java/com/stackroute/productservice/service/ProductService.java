package com.stackroute.productservice.service;

import com.stackroute.productservice.errorhandler.exception.ProductExistsException;
import com.stackroute.productservice.errorhandler.exception.ProductNotFoundException;
import com.stackroute.productservice.model.Product;

import java.util.List;

public interface ProductService {

    Product addNewProduct(Product product) throws ProductExistsException;

    Product fetchProductByProductId(String productId) throws ProductNotFoundException;

    List<Product> fetchProductsByCategory(String category);

    Product updateProduct(Product product, String productId) throws ProductNotFoundException;

    Product removeProductById(String productId) throws ProductNotFoundException;
}
