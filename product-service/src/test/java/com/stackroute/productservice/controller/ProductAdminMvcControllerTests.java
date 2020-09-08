package com.stackroute.productservice.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stackroute.productservice.config.ProductServiceConfig;
import com.stackroute.productservice.dto.ProductDto;
import com.stackroute.productservice.model.Product;
import com.stackroute.productservice.service.ProductService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(controllers = ProductAdminMvcController.class)
@Import({ProductServiceConfig.class})
public class ProductAdminMvcControllerTests {

    private final ObjectMapper mapper = new ObjectMapper();
    @Autowired
    private MockMvc mockMvc;
    @MockBean
    private ProductService productService;
    @Autowired
    private ModelMapper modelMapper;
    private ProductDto productDto;
    private Product product;
    private String productJson;

    @BeforeEach
    public void setUp() throws IOException {
        productDto = new ProductDto("1", "Carrot", "Organic Carrot", "Vegetables", 5.5, "http://test.com/testurl");
        product = new Product("1", "Carrot", "Organic Carrot", "Vegetables", 5.5, "http://test.com/testurl");
    }


    @Test
    @WithMockUser
    public void givenProductCategoryThenReturnProductsAndViewName() throws Exception {
        when(productService.fetchProductsByCategory(any(String.class)))
                .thenReturn(List.of(product));
        mockMvc.perform(get("/admin/products")
                .param("category", "Vegetables")
        )
                .andExpect(status().isOk())
                .andExpect(view().name("products_administration"))
                .andExpect(model().attribute("products", hasSize(1)))
                .andExpect(model().attribute("products", hasItem(product)));


        verify(productService, times(1)).fetchProductsByCategory(any(String.class));

    }
}