package com.stackroute.productservice.controller;

import com.stackroute.productservice.dto.ProductDto;
import com.stackroute.productservice.errorhandler.exception.ProductExistsException;
import com.stackroute.productservice.errorhandler.exception.ProductNotFoundException;
import com.stackroute.productservice.model.Product;
import com.stackroute.productservice.service.ProductService;
import org.modelmapper.ModelMapper;
import org.modelmapper.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.lang.reflect.Type;
import java.util.List;

@RestController
@RequestMapping("api")
public class ProductController {

    /**
     * **TODO**
     * Create a slf4j Logger for logging messages to standard output
     */
    private Logger log = LoggerFactory.getLogger(ProductController.class);

    private final ProductService productService;
    private final ModelMapper modelMapper;

    @Autowired
    public ProductController(ProductService productService, ModelMapper modelMapper) {
        this.productService = productService;
        this.modelMapper = modelMapper;
    }

    /**
     * REST Endpoint for adding new product
     * URI: /api/v1/products  METHOD: POST
     * Response status: success: 201(created) , Product already exists : 409(conflict)
     *
     * **TODO**
     * Log the informational message in the below method on successful creation of product
     *  "New product added with id <<productId>>"
     */
    @PostMapping("v1/products")
    @ResponseStatus(HttpStatus.CREATED)
    public ProductDto addNewProduct(@Valid @RequestBody ProductDto productDto) throws ProductExistsException {
        Product product = convertToEntity(productDto);
        Product addedProduct = productService.addNewProduct(product);
        log.info("New product added with id "+addedProduct.getId());
        return convertToDto(addedProduct);
    }

    /**
     * REST Endpoint for fetching a product based on product id
     * URI: /api/v1/products/{productId}  METHOD: GET
     * Response status: success: 200(success) , Product not found : 404(not found)
     */
    @GetMapping("v1/products/{productId}")
    public ProductDto fetchProductById(@PathVariable String productId) throws ProductNotFoundException {
        return convertToDto(productService.fetchProductByProductId(productId));
    }

    /**
     * REST Endpoint for fetching products based on category
     * URI: /api/v1/products/category/{category}  METHOD: GET
     * Response status: success: 200(success)
     *
     * **TODO**
     * Log the warning message in the below method if there are no product
     * in the category given
     *  "No products in the category <<category name>>"
     */
    @GetMapping("v1/products/category/{category}")
    public List<ProductDto> fetchProductsByCategory(@PathVariable String category) {
        List<Product> products = productService.fetchProductsByCategory(category);
        if(products.size() == 0){
            log.warn("No products in the category "+category);
        }
        return convertToDtoList(products);
    }

    /**
     * REST Endpoint for fetching a product based on product id
     * URI: /api/v1/products/{productId}  METHOD: PUT
     * Response status: success: 200(success) , Product not found : 404(not found)
     *
     * **TODO**
     * Log the informational message in the below method on successful updation of product
     *  "Product details updated for product id <<productId>>"
     */
    @PutMapping("v1/products/{productId}")
    @ResponseStatus(HttpStatus.OK)
    public ProductDto updateProduct(@Valid @RequestBody ProductDto productDto, @PathVariable String productId) throws ProductNotFoundException {
        Product updatedProduct = productService.updateProduct(convertToEntity(productDto), productId);
        log.info("Product details updated for product id "+updatedProduct.getId());
        return convertToDto(updatedProduct);
    }

    /**
     * REST Endpoint for deleting a product based on product id
     * URI: /api/v1/products/{productId}  METHOD: DELETE
     * Response status: success: 204(No Content) , Product not found : 404(not found)
     *
     *  **TODO**
     * Log the warning message in the below method if there are no product
     * in the category given
     *  "Deleted product with product id <<productId>>"
     */
    @DeleteMapping("v1/products/{productId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeProduct(@PathVariable String productId) throws ProductNotFoundException {
        productService.removeProductById(productId);
        log.warn("Deleted product with product id "+productId);
    }

    /**
     * Method for converting Product entity to ProductDto object
     */
    private ProductDto convertToDto(Product product) {
        return modelMapper.map(product, ProductDto.class);
    }

    /**
     * Method for converting ProductDto to Product entity object
     */
    private Product convertToEntity(@RequestBody @Valid ProductDto productDto) {
        return modelMapper.map(productDto, Product.class);
    }

    /**
     * Method for converting List of Product entity to List of ProductDto object
     */
    private List<ProductDto> convertToDtoList(List<Product> products) {
        Type listType = new TypeToken<List<ProductDto>>() {
        }.getType();
        return modelMapper.map(products, listType);
    }
}