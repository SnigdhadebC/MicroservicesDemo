package com.stackroute.productservice.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class UserDetailsImplTests {

    private User user;
    private UserDetailsImpl userDetails;

    @BeforeEach
    public void setUp() {
        user = User.builder()
                .username("admin")
                .password("password")
                .active(true)
                .role("ADMIN")
                .build();

        userDetails = new UserDetailsImpl(user);
    }

    @Test
    public void givenUserDetailsImplObjectThenIsAnInstanceOfUserDetails() {
        UserDetailsImpl userDetails = new UserDetailsImpl();
        assertThat(userDetails).isInstanceOf(UserDetails.class);
    }

    @Test
    public void givenUserDetailsPropertiesWhenObjectCreatedThenPropertiesInitialized() {
        assertThat(userDetails)
                .hasFieldOrPropertyWithValue("username", "admin")
                .hasFieldOrPropertyWithValue("password", "password")
                .hasFieldOrPropertyWithValue("active", true)
                .hasFieldOrPropertyWithValue("authorities",
                        List.of(new SimpleGrantedAuthority(user.getRole())));
    }

    @Test
    public void givenUserDetailsObjectThenReturnAccountNonExpired() {
        assertThat(userDetails.isAccountNonExpired())
                .isTrue();
    }

    @Test
    public void givenUserDetailsObjectThenReturnAccountNonLocked() {
        assertThat(userDetails.isAccountNonLocked())
                .isTrue();
    }

    @Test
    public void givenUserDetailsObjectThenReturnUserCredentialsNonExpired() {
        assertThat(userDetails.isCredentialsNonExpired())
                .isTrue();
    }

    @Test
    public void givenUserDetailsObjectThenReturnUserIsActive() {
        assertThat(userDetails.isEnabled())
                .isEqualTo(user.isActive());
    }


}