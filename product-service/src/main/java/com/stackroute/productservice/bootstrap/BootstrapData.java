package com.stackroute.productservice.bootstrap;

import com.stackroute.productservice.model.Product;
import com.stackroute.productservice.model.User;
import com.stackroute.productservice.repository.ProductRepository;
import com.stackroute.productservice.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.security.crypto.bcrypt.BCrypt;
import org.springframework.stereotype.Component;

import java.util.List;


/**
 * This class is used to populate the User Table with an user named `admin`
 * during application start
 */

@Component
public class BootstrapData implements ApplicationListener<ContextRefreshedEvent> {

    private final Logger logger = LoggerFactory.getLogger(BootstrapData.class);

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private ProductRepository productRepository;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        User user1 = User.builder()
                .username("admin")
                .password(BCrypt.hashpw("admin", BCrypt.gensalt()))
                .active(true)
                .role("ADMIN")
                .build();

        if (!userRepository.existsByUsername(user1.getUsername())) {
            logger.info("Adding admin user to database during application start");
            userRepository.save(user1);
        }

        logger.info("Adding initial products to database during application start");
        addProducts();
    }

    private void addProducts(){
        Product product1 = new Product("FR001", "Mango", "Alphonso Mango", "Fruits", 10.5, "http://test.com/testurl1");
        Product product2 = new Product("CR001", "Wheat", "organic wheat", "Cereals", 35, "http://test.com/testurl3");
        Product product3 = new Product("FR002", "Apple", "Green Apple", "Fruits", 12.5, "http://test.com/testurl4");
        List<Product> products = List.of(product1,product2,product3);
        productRepository.saveAll(products);
    }
}
