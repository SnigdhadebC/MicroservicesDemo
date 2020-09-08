package com.stackroute.productservice.repository;

import com.stackroute.productservice.model.User;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@ExtendWith(SpringExtension.class)
public class UserRepositoryEmbeddedH2Tests {

    @Autowired
    private UserRepository userRepository;

    @BeforeEach
    public void dataSetUp() {
        User user1 = User.builder()
                .username("admin")
                .password("pass1")
                .active(true)
                .role("ADMIN")
                .build();
        userRepository.save(user1);
    }

    @AfterEach
    public void tearDown() {
        userRepository.deleteAll();
    }

    @Test
    public void givenUsernameThenReturnOptionalOfUser() {
        Optional<User> optionalUser = userRepository.findUserByUsername("admin");
        assertThat(optionalUser).isNotEmpty();
        assertThat(optionalUser).hasValueSatisfying(user -> {
            assertThat(user.getPassword()).isEqualTo("pass1");
            assertThat(user.getRole()).isEqualTo("ADMIN");
            assertThat(user.isActive()).isTrue();
        });
    }

    @Test
    public void givenUsernameThenReturnUserExists() {
        boolean adminPresent = userRepository.existsByUsername("admin");
        assertThat(adminPresent).isTrue();
    }


}