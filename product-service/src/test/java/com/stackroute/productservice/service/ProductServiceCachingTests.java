package com.stackroute.productservice.service;

import com.stackroute.productservice.errorhandler.exception.ProductExistsException;
import com.stackroute.productservice.errorhandler.exception.ProductNotFoundException;
import com.stackroute.productservice.model.Product;
import com.stackroute.productservice.repository.ProductRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cache.support.SimpleCacheManager;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@Import({TestCacheConfiguration.class, ProductServiceImpl.class})
public class ProductServiceCachingTests {

    public static final String VEGETABLES = "Vegetables";
    public static final String EVICTED_FROM_CATEGORY_PRODUCTS = "List of products should be evicted from categoryProducts cache, when product is added/updated in a category";
    public static final String RETRIEVED_FROM_PRODUCTS_CACHE = "Caching not working. Product should have been retrieved from products cache for the second call";
    public static final String CATEGORY_PRODUCTS = "categoryProducts";

    @MockBean
    private ProductRepository productRepository;

    @Autowired
    private ProductService productService;

    @Autowired
    private SimpleCacheManager cacheManager;

    private Product product;
    private Optional<Product> optionalProduct;
    private Optional<Product> emptyOptional;


    @BeforeEach
    public void setUp() {
        product = new Product("1", "Carrot", "Organic Carrot", VEGETABLES, 5.5, "http://test.com/testurl");
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
    public void givenProductObjectWhenProductCreatedThenProductAddedToProductsCache() throws ProductExistsException, ProductNotFoundException {
        product.setId("2");
        when(productRepository.findById("2"))
                .thenReturn(emptyOptional);
        when(productRepository.save(product))
                .thenReturn(product);
        Product addedProduct = productService.addNewProduct(product);
        productService.fetchProductByProductId("2");
        verify(productRepository, times(1)
                .description(RETRIEVED_FROM_PRODUCTS_CACHE)).findById("2");
    }

    @Test
    public void givenProductObjectWhenProductCreatedThenProductsOfThatCategoryRemovedFromCategoryProductsCache() throws ProductExistsException, ProductNotFoundException {
        product.setId("3");
        when(productRepository.findById("3"))
                .thenReturn(emptyOptional);
        when(productRepository.save(product))
                .thenReturn(product);
        Product addedProduct = productService.addNewProduct(product);
        verify(productRepository, times(1)).findById("3");
        assertThat(cacheManager.getCache(CATEGORY_PRODUCTS).get(VEGETABLES))
                .as(EVICTED_FROM_CATEGORY_PRODUCTS)
                .isNull();

    }

    @Test
    public void givenProductCategoryWhenProductsFetchedThenListAddedToCategoryCache() {
        Product product2 = new Product("1", "Tomato", "Organic Tomato", VEGETABLES, 15.5, "http://test.com/testurl");
        List<Product> products = List.of(product, product2);
        when(productRepository.findByCategoryIgnoreCase(VEGETABLES))
                .thenReturn(products);
        productService.fetchProductsByCategory(VEGETABLES);
        productService.fetchProductsByCategory(VEGETABLES);
        verify(productRepository, times(1)).findByCategoryIgnoreCase(VEGETABLES);
    }

    @Test
    public void givenProductIdWhenProductFetchedThenAddedToProductCache() throws ProductNotFoundException {
        when(productRepository.findById("1")).thenReturn(optionalProduct);
        assertThat(productService.fetchProductByProductId("1"))
                .isNotNull();
        assertThat(productService.fetchProductByProductId("1"))
                .isNotNull();
        verify(productRepository, times(1)
                .description(RETRIEVED_FROM_PRODUCTS_CACHE))
                .findById("1");
    }

    @Test
    public void givenProductObjectWhenProductUpdatedThenReplaceInProductsCacheAndEvictFromCategoryProductsCache() throws ProductNotFoundException {
        product.setDescription("New Organic Carrot");
        product.setId("4");
        when(productRepository.existsById("4"))
                .thenReturn(true);
        when(productRepository.save(product))
                .thenReturn(product);
        Product updatedProduct = productService.updateProduct(product, "4");
        assertThat(updatedProduct).isNotNull();
        productService.fetchProductByProductId("4");
        verify(productRepository, times(1)
                .description(RETRIEVED_FROM_PRODUCTS_CACHE))
                .existsById("4");
        assertThat(cacheManager.getCache(CATEGORY_PRODUCTS).get(VEGETABLES))
                .as(EVICTED_FROM_CATEGORY_PRODUCTS)
                .isNull();
    }

    @Test
    public void givenProductIdWhenProductRemovedThenEvictedFromProductsCacheAndCategoryProductsCache() throws ProductNotFoundException {
        when(productRepository.findById("1"))
                .thenReturn(optionalProduct);
        Product deletedProduct = productService.removeProductById("1");
        assertThat(deletedProduct).isNotNull();
        productService.fetchProductByProductId("1");
        verify(productRepository, times(2)
                .description(RETRIEVED_FROM_PRODUCTS_CACHE))
                .findById("1");
        assertThat(cacheManager.getCache(CATEGORY_PRODUCTS).get(VEGETABLES))
                .as(EVICTED_FROM_CATEGORY_PRODUCTS)
                .isNull();
    }
}