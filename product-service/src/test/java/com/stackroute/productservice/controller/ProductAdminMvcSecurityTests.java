package com.stackroute.productservice.controller;

import com.stackroute.productservice.config.ProductServiceConfig;
import com.stackroute.productservice.config.SecurityConfiguration;
import com.stackroute.productservice.dto.ProductDto;
import com.stackroute.productservice.model.Product;
import com.stackroute.productservice.model.User;
import com.stackroute.productservice.service.ProductService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.security.test.context.support.WithUserDetails;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.io.IOException;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestBuilders.formLogin;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestBuilders.logout;
import static org.springframework.security.test.web.servlet.response.SecurityMockMvcResultMatchers.unauthenticated;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = ProductAdminMvcController.class)
@Import({ProductServiceConfig.class, SecurityConfiguration.class})
public class ProductAdminMvcSecurityTests {

    private MockMvc mockMvc;

    @MockBean
    private ProductService productService;

    @Autowired
    private ModelMapper modelMapper;


    private ProductDto productDto;
    private Product product;
    private User user;

    @Autowired
    private WebApplicationContext webApplicationContext;


    @BeforeEach
    public void setUp() throws IOException {
        mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext)
                .apply(springSecurity())
                .build();
        user = User.builder()
                .username("admin")
                .password("password")
                .active(true)
                .role("ADMIN")
                .build();

        productDto = new ProductDto("1", "Carrot", "Organic Carrot", "Vegetables", 5.5, "http://test.com/testurl");
        product = new Product("1", "Carrot", "Organic Carrot", "Vegetables", 5.5, "http://test.com/testurl");

    }

    @Test
    @WithUserDetails
    public void givenValidUserDetailsThenReturnListOfProductsByCategory() throws Exception {
        when(productService.fetchProductsByCategory(any(String.class)))
                .thenReturn(List.of(product));
        mockMvc.perform(get("/admin/products")
                .param("category", "Vegetables")
        )
                .andExpect(status().isOk());
    }

    @Test
    @WithMockUser
    public void givenMockUserThenLoginSuccessful() throws Exception {
        mockMvc.perform(formLogin().loginProcessingUrl("/admin/processlogin"))
                .andExpect(status().isFound());
    }

    @Test
    public void givenInvalidPasswordForUserWhenLoginDoneThenUserUnauthenticated() throws Exception {
        mockMvc.perform(formLogin().password("invalid"))
                .andExpect(unauthenticated());
    }

    @Test
    public void givenLoggedInUserWhenLoggedOutThenLogoutSuccessful() throws Exception {
        mockMvc.perform(logout())
                .andExpect(status().isFound());
    }
}