package com.stackroute.productservice.integrationtest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stackroute.productservice.ProductServiceApplication;
import com.stackroute.productservice.dto.ProductDto;
import com.stackroute.productservice.model.Product;
import com.stackroute.productservice.service.TestCacheConfiguration;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.support.SimpleCacheManager;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = ProductServiceApplication.class
)
@AutoConfigureMockMvc
@Import({TestCacheConfiguration.class})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestPropertySource(properties = "spring.data.mongodb.port=27020")
@DirtiesContext
public class ProductServiceITTests {
    public static final String VEGETABLES = "Vegetables";
    public static final String EVICTED_FROM_CATEGORY_PRODUCTS = "List of products should be evicted from categoryProducts cache, when product is added/updated in a category";
    public static final String RETRIEVED_FROM_PRODUCTS_CACHE = "Caching not working. Product should have been retrieved from products cache for the second call";
    public static final String CATEGORY_PRODUCTS = "categoryProducts";
    public static final String ID_ALREADY_EXISTS = "Product with given id already exists";
    public static final String PRODUCTS_BASE_URL = "/api/v1/products";
    public static final String PRODUCTS = "products";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private SimpleCacheManager cacheManager;

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
    @Order(1)
    public void givenNewProductJsonWhenProductCreatedThenReturnCreatedStatus() throws Exception {
        MvcResult mvcResult = mockMvc.perform(
                post(PRODUCTS_BASE_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(productJson))
                .andExpect(status().isCreated())
                .andReturn();
        ProductDto responseProduct = toObjectFromJson(mvcResult, ProductDto.class);
        assertThat(responseProduct).isEqualTo(productDto);
        assertThat(cacheManager.getCache(PRODUCTS).get(productDto.getId()))
                .isNotNull()
                .isEqualToComparingFieldByField(new SimpleValueWrapper(product));
    }


    @Test
    @Order(2)
    public void givenNewProductJsonWhenProductExistsThenReturnConflictStatus() throws Exception {
        mockMvc.perform(
                post(PRODUCTS_BASE_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(productJson))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message").value(ID_ALREADY_EXISTS));
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
    }


    @Test
    @Order(2)
    public void givenProductIdWhenProductExistsThenReturnProductJson() throws Exception {

        MvcResult mvcResult = mockMvc.perform(
                get("/api/v1/products/{productId}", "1"))
                .andExpect(status().isOk())
                .andReturn();


        ProductDto productResponse = toObjectFromJson(mvcResult, ProductDto.class);
        assertThat(productResponse).isEqualTo(productDto);
    }

    @Test
    @Order(2)
    public void givenProductCategoryThenReturnJsonWithProductsOfGivenCategory() throws Exception {
        mockMvc.perform(
                get("/api/v1/products/category/{category}", "Vegetables"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", Matchers.hasSize(1)))
                .andExpect(jsonPath("$[*].name", Matchers.containsInAnyOrder("Carrot")));

        assertThat(cacheManager.getCache(CATEGORY_PRODUCTS).get(productDto.getCategory()))
                .isNotNull();
    }

    @Test
    @Order(3)
    public void givenValidProductJsonWhenProductUpdatedThenReturnOKStatus() throws Exception {
        product.setDescription("testUpdate");
        productJson = toJson(product);
        productDto.setDescription("testUpdate");

        MvcResult mvcResult = mockMvc.perform(
                put(PRODUCTS_BASE_URL + "/{productId}", "1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(productJson))
                .andExpect(status().isOk())
                .andReturn();
        ProductDto responseProduct = toObjectFromJson(mvcResult, ProductDto.class);
        assertThat(responseProduct).isEqualTo(productDto);
    }

    @Test
    @Order(5)
    public void givenProductIdWhenProductDeletedThenReturnNoContentStatus() throws Exception {

        MvcResult mvcResult = mockMvc.perform(
                delete(PRODUCTS_BASE_URL + "/{productId}", "1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(productJson))
                .andExpect(status().isNoContent())
                .andReturn();
        assertThat(cacheManager.getCache(PRODUCTS).get(productDto.getId()))
                .isNull();
    }

    private String toJson(final Object obj) throws JsonProcessingException {
        return this.mapper.writeValueAsString(obj);
    }

    <T> T toObjectFromJson(MvcResult result, Class<T> toClass) throws Exception {
        return this.mapper.readValue(result.getResponse().getContentAsString(), toClass);
    }
}