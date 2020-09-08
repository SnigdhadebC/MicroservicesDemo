package com.stackroute.productservice.service;

import com.stackroute.productservice.errorhandler.exception.ProductExistsException;
import com.stackroute.productservice.errorhandler.exception.ProductNotFoundException;
import com.stackroute.productservice.model.Product;
import com.stackroute.productservice.repository.ProductRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ProductServiceImplTests {

    @Mock
    private ProductRepository productRepository;

    @InjectMocks
    private ProductServiceImpl productService;

    private Product product;
    private Optional<Product> optionalProduct;
    private Optional<Product> emptyOptional;


    @BeforeEach
    public void setUp() {
        product = new Product("1", "Carrot", "Organic Carrot", "Vegetables", 5.5, "http://test.com/testurl");
        optionalProduct = Optional.of(product);
        emptyOptional = Optional.empty();
    }

    @AfterEach
    public void tearDown() {
        product = null;
        optionalProduct = null;
        emptyOptional = null;
    }

    @Test
    public void givenProductEntityObjectThenNewProductCreated() throws ProductExistsException {
        when(productRepository.findById("1"))
                .thenReturn(emptyOptional);
        when(productRepository.save(product))
                .thenReturn(product);
        Product addedProduct = productService.addNewProduct(this.product);
        assertThat(addedProduct).isEqualTo(product);
        verify(productRepository, times(1)).save(product);
    }

    @Test
    public void givenProductEntityObjectWhenAlreadyExistsThenNewProductCreated() throws ProductExistsException {
        when(productRepository.findById("1"))
                .thenReturn(optionalProduct);
        assertThatThrownBy(() -> productService.addNewProduct(product))
                .isInstanceOf(ProductExistsException.class)
                .hasMessageContaining("Product with given id already exists");
        verify(productRepository, times(1)).findById("1");
    }

    @Test
    public void givenProductIdWhenFetchedThenReturnProduct() throws ProductNotFoundException {
        when(productRepository.findById("1")).thenReturn(optionalProduct);
        Product existingProduct = productService.fetchProductByProductId("1");
        assertThat(existingProduct).isEqualTo(product);
        verify(productRepository, times(1)).findById("1");
    }

    @Test
    public void givenProductIdWhenProductDoesNotExistThenThrowException() throws ProductNotFoundException {
        when(productRepository.findById("1"))
                .thenReturn(emptyOptional);
        assertThatThrownBy(() -> productService.fetchProductByProductId("1"))
                .isInstanceOf(ProductNotFoundException.class)
                .hasMessageContaining("Product not found");
        verify(productRepository, times(1)).findById("1");
    }

    @Test
    public void givenProductCategoryThenReturnListOfProducts() {
        Product product2 = new Product("1", "Tomato", "Organic Tomato", "Vegetables", 15.5, "http://test.com/testurl");
        List<Product> products = List.of(product, product2);
        when(productRepository.findByCategoryIgnoreCase("Vegetables"))
                .thenReturn(products);
        assertThat(productService.fetchProductsByCategory("Vegetables"))
                .hasSize(2)
                .extracting(Product::getCategory)
                .containsOnly("Vegetables");
        verify(productRepository, times(1)).findByCategoryIgnoreCase("Vegetables");
    }

    @Test
    public void givenProductObjectThenUpdateSuccessful() throws ProductNotFoundException {
        product.setDescription("New Organic Carrot");
        when(productRepository.existsById("1"))
                .thenReturn(true);
        when(productRepository.save(product))
                .thenReturn(product);
        Product updatedProduct = productService.updateProduct(product, "1");
        assertThat(updatedProduct).isEqualTo(product);
        verify(productRepository, times(1)).existsById("1");
        verify(productRepository, times(1)).save(product);
    }

    @Test
    public void givenProductIdThenProductRemoved() throws ProductNotFoundException {
        when(productRepository.findById("1"))
                .thenReturn(optionalProduct);
        Product deletedProduct = productService.removeProductById("1");
        assertThat(deletedProduct).isEqualTo(product);
        verify(productRepository, times(1)).findById("1");
    }
}