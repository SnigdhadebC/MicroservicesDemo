package com.stackroute.productservice.service;

import com.stackroute.productservice.model.User;
import com.stackroute.productservice.repository.UserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


@ExtendWith(MockitoExtension.class)
public class UserDetailsServiceImplTests {

    @Mock
    private UserRepository userRepository;

    @InjectMocks
    private UserDetailsServiceImpl userDetailsService;

    private Optional<User> optionalUser;


    @BeforeEach
    public void setUp() {
        User user = User.builder()
                .username("admin")
                .password("password")
                .active(true)
                .role("ADMIN")
                .build();
        optionalUser = Optional.of(user);
    }

    @Test
    public void givenUserDetailsServiceImplObjectThenIsAnInstanceOfUserDetailsService() {
        assertThat(userDetailsService).isInstanceOf(UserDetailsService.class);
    }

    @Test
    public void givenUserDetailsServicePropertiesWhenObjectCreatedThenPropertiesInitialized() {
        UserDetailsServiceImpl userDetailsService = new UserDetailsServiceImpl(userRepository);
        assertThat(userDetailsService)
                .hasFieldOrPropertyWithValue("userRepository", userRepository);
    }

    @Test
    public void givenUsernameThenLoadUserByUsername() {
        Mockito.when(userRepository.findUserByUsername("admin")).thenReturn(optionalUser);

        UserDetails userDetails = userDetailsService.loadUserByUsername("admin");
        assertThat(userDetails.getUsername()).isEqualTo("admin");
        assertThat(userDetails.getPassword()).isEqualTo("password");
        assertThat(userDetails.isEnabled()).isTrue();
    }

    @Test
    public void givenUsernameWhenDoesNotExistThenThrowException() {
        Mockito.when(userRepository.findUserByUsername("admin")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> userDetailsService.loadUserByUsername("admin"))
                .isInstanceOf(UsernameNotFoundException.class);
    }
}