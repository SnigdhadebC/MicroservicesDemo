package com.stackroute.productservice.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stackroute.productservice.config.ProductServiceConfig;
import com.stackroute.productservice.dto.ProductDto;
import com.stackroute.productservice.errorhandler.exception.ProductExistsException;
import com.stackroute.productservice.model.Product;
import com.stackroute.productservice.service.ProductService;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = ProductController.class)
@Import(ProductServiceConfig.class)
public class ProductControllerTests {

    public static final String ID_ALREADY_EXISTS = "Product with given id already exists";
    public static final String PRODUCTS_BASE_URL = "/api/v1/products";
    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private ProductService productService;

    @Autowired
    private ModelMapper modelMapper;

    @Autowired
    private ObjectMapper mapper;

    private ProductDto productDto;
    private Product product;
    private String productJson;

    @BeforeEach
    public void setUp() throws IOException {
        productDto = new ProductDto("1", "Carrot", "Organic Carrot", "Vegetables", 5.5, "http://test.com/testurl");
        productJson = toJson(productDto);
        product = new Product("1", "Carrot", "Organic Carrot", "Vegetables", 5.5, "http://test.com/testurl");
    }

    @AfterEach
    public void tearDown() {
        productDto = null;
        product = null;
        productJson = null;
    }

    @Test
    public void givenNewProductJsonWhenProductCreatedThenReturnCreatedStatus() throws Exception {
        when(productService.addNewProduct(any(Product.class)))
                .thenReturn(product);
        MvcResult mvcResult = mockMvc.perform(
                post(PRODUCTS_BASE_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(productJson))
                .andExpect(status().isCreated())
                .andReturn();
        ProductDto responseProduct = toObjectFromJson(mvcResult, ProductDto.class);
        assertThat(responseProduct).isEqualTo(productDto);
        verify(productService, times(1)).addNewProduct(any(Product.class));
    }

    @Test
    public void givenNewProductJsonWhenProductExistsThenReturnConflictStatus() throws Exception {
        when(productService.addNewProduct(any(Product.class)))
                .thenThrow(new ProductExistsException(ID_ALREADY_EXISTS));
        mockMvc.perform(
                post(PRODUCTS_BASE_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(productJson))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message").value(ID_ALREADY_EXISTS));
        verify(productService, times(1)).addNewProduct(any(Product.class));
    }

    @Test
    public void givenNewProductJsonWhenProductDetailsInvalidThenReturnBadRequestStatus() throws Exception {

        productDto.setId("");
        mockMvc.perform(
                post(PRODUCTS_BASE_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(productDto)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Validation of Request failed"))
                .andExpect(jsonPath("$.errors[0]").value("Product id is empty or null"));
        verifyNoInteractions(productService);
    }

    @Test
    public void givenProductIdWhenProductExistsThenReturnProductJson() throws Exception {
        when(productService.fetchProductByProductId(any(String.class)))
                .thenReturn(product);
        MvcResult mvcResult = mockMvc.perform(
                get("/api/v1/products/{productId}", "1"))
                .andExpect(status().isOk())
                .andReturn();


        ProductDto productResponse = toObjectFromJson(mvcResult, ProductDto.class);
        assertThat(productResponse).isEqualTo(productDto);
        verify(productService, times(1))
                .fetchProductByProductId(any(String.class));

    }

    @Test
    public void givenProductCategoryThenReturnJsonWithProductsOfGivenCategory() throws Exception {
        Product product2 = new Product("2", "Tomato", "Organic Tomato", "Vegetables", 15.5, "http://test.com/testurl");
        List<Product> products = List.of(product, product2);

        when(productService.fetchProductsByCategory(any(String.class)))
                .thenReturn(products);
        mockMvc.perform(
                get("/api/v1/products/category/{category}", "Vegetables"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", Matchers.hasSize(2)))
                .andExpect(jsonPath("$[*].name", Matchers.containsInAnyOrder("Tomato", "Carrot")));

        verify(productService, times(1)).fetchProductsByCategory(any(String.class));
    }

    @Test
    public void givenValidProductJsonWhenProductUpdatedThenReturnOKStatus() throws Exception {
        product.setDescription("testUpdate");
        productDto.setDescription("testUpdate");
        when(productService.updateProduct(any(Product.class), anyString()))
                .thenReturn(product);
        MvcResult mvcResult = mockMvc.perform(
                put(PRODUCTS_BASE_URL + "/{productId}", "1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(productJson))
                .andExpect(status().isOk())
                .andReturn();
        ProductDto responseProduct = toObjectFromJson(mvcResult, ProductDto.class);
        assertThat(responseProduct).isEqualTo(productDto);
        verify(productService, times(1)).updateProduct(any(Product.class), anyString());
    }

    @Test
    public void givenProductIdWhenProductDeletedThenReturnNoContentStatus() throws Exception {

        when(productService.removeProductById(anyString()))
                .thenReturn(product);
        MvcResult mvcResult = mockMvc.perform(
                delete(PRODUCTS_BASE_URL + "/{productId}", "1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(productJson))
                .andExpect(status().isNoContent())
                .andReturn();
        verify(productService, times(1)).removeProductById(anyString());

    }

    private String toJson(final Object obj) throws JsonProcessingException {
        return this.mapper.writeValueAsString(obj);
    }

    <T> T toObjectFromJson(MvcResult result, Class<T> toClass) throws Exception {
        return this.mapper.readValue(result.getResponse().getContentAsString(), toClass);
    }

}