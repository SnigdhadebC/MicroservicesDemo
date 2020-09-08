package com.stackroute.productservice.controller;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stackroute.productservice.StaticAppender;
import com.stackroute.productservice.config.ProductServiceConfig;
import com.stackroute.productservice.dto.ProductDto;
import com.stackroute.productservice.errorhandler.exception.ProductExistsException;
import com.stackroute.productservice.errorhandler.exception.ProductNotFoundException;
import com.stackroute.productservice.model.Product;
import com.stackroute.productservice.service.ProductService;
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
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = ProductController.class)
@Import(ProductServiceConfig.class)
public class ProductControllerLoggingTests {

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
        StaticAppender.clearEvents();
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
    public void givenNewProductJsonWhenProductCreatedThenLogInfoMessage() throws Exception {
        when(productService.addNewProduct(any(Product.class)))
                .thenReturn(product);
        mockMvc.perform(
                post(PRODUCTS_BASE_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(productJson))
                .andExpect(status().isCreated());
        List<ILoggingEvent> loggingEvents = StaticAppender.getEvents();
        assertThat(loggingEvents).extracting(ILoggingEvent::getLevel).contains(Level.INFO);
        assertThat(loggingEvents).extracting(ILoggingEvent::getMessage).anyMatch(msg -> msg.replaceAll("\\s+", "").toLowerCase().contains("newproductaddedwithid"));
    }

    @Test
    public void givenNewProductJsonWhenProductExistsThenLogErrorMessage() throws Exception {
        when(productService.addNewProduct(any(Product.class)))
                .thenThrow(new ProductExistsException(ID_ALREADY_EXISTS));
        mockMvc.perform(
                post(PRODUCTS_BASE_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(productJson))
                .andExpect(status().isConflict());
        assertThat(StaticAppender.getEvents()).extracting(ILoggingEvent::getLevel)
                .contains(Level.ERROR);
        assertThat(StaticAppender.getEvents()).extracting(ILoggingEvent::getMessage)
                .anyMatch(msg -> msg.replaceAll("\\s+", "").toLowerCase()
                        .contains("buildingerrorresponseforproductexistsexception"));
    }

    @Test
    public void givenNewProductJsonWhenProductDetailsInvalidThenLogErrorMessage() throws Exception {

        productDto.setId("");
        mockMvc.perform(
                post(PRODUCTS_BASE_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(productDto)))
                .andExpect(status().isBadRequest());
        assertThat(StaticAppender.getEvents()).extracting(ILoggingEvent::getLevel)
                .contains(Level.ERROR);
        assertThat(StaticAppender.getEvents()).extracting(ILoggingEvent::getMessage)
                .anyMatch(msg -> msg.replaceAll("\\s+", "").toLowerCase()
                        .contains("buildingerrorresponseforinvalidrequestparameters"));


    }

    @Test
    public void givenProductCategoryWhenNoProductsExistThenLogWarningMessage() throws Exception {
        List<Product> products = Collections.emptyList();

        when(productService.fetchProductsByCategory(any(String.class)))
                .thenReturn(products);
        mockMvc.perform(
                get("/api/v1/products/category/{category}", "Vegetables"))
                .andExpect(status().isOk());
        assertThat(StaticAppender.getEvents()).extracting(ILoggingEvent::getLevel)
                .contains(Level.WARN);
        assertThat(StaticAppender.getEvents()).extracting(ILoggingEvent::getMessage)
                .anyMatch(msg -> msg.replaceAll("\\s+", "").toLowerCase()
                        .contains("noproductsinthecategory"));

    }

    @Test
    public void givenValidProductJsonWhenProductUpdatedThenLogInfoMessage() throws Exception {
        product.setDescription("testUpdate");
        productDto.setDescription("testUpdate");
        when(productService.updateProduct(any(Product.class), anyString()))
                .thenReturn(product);
        mockMvc.perform(
                put(PRODUCTS_BASE_URL + "/{productId}", "1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(productJson))
                .andExpect(status().isOk());
        assertThat(StaticAppender.getEvents()).extracting(ILoggingEvent::getLevel)
                .contains(Level.INFO);
        assertThat(StaticAppender.getEvents()).extracting(ILoggingEvent::getMessage)
                .anyMatch(msg -> msg.replaceAll("\\s+", "").toLowerCase()
                        .contains("productdetailsupdatedforproductid"));

    }

    @Test
    public void givenProductIdWhenProductDeletedThenLogWarningMessage() throws Exception {

        when(productService.removeProductById(anyString()))
                .thenReturn(product);
        mockMvc.perform(
                delete(PRODUCTS_BASE_URL + "/{productId}", "1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(productJson))
                .andExpect(status().isNoContent());

        assertThat(StaticAppender.getEvents()).extracting(ILoggingEvent::getLevel)
                .contains(Level.WARN);
        assertThat(StaticAppender.getEvents()).extracting(ILoggingEvent::getMessage)
                .anyMatch(msg -> msg.replaceAll("\\s+", "").toLowerCase()
                        .contains("deletedproductwithproductid"));
    }

    @Test
    public void givenProductIdWhenProductNotFoundThenLogErrorMessage() throws Exception {
        when(productService.fetchProductByProductId(anyString()))
                .thenThrow(ProductNotFoundException.class);
        mockMvc.perform(
                get(PRODUCTS_BASE_URL + "/{productId}", "1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(productJson))
                .andExpect(status().isNotFound());

        assertThat(StaticAppender.getEvents()).extracting(ILoggingEvent::getLevel)
                .contains(Level.ERROR);
        assertThat(StaticAppender.getEvents()).extracting(ILoggingEvent::getMessage)
                .anyMatch(msg -> msg.replaceAll("\\s+", "").toLowerCase()
                        .contains("buildingerrorresponseforproductnotfoundexception"));
    }


    private String toJson(final Object obj) throws JsonProcessingException {
        return this.mapper.writeValueAsString(obj);
    }

    <T> T toObjectFromJson(MvcResult result, Class<T> toClass) throws Exception {
        return this.mapper.readValue(result.getResponse().getContentAsString(), toClass);
    }

}