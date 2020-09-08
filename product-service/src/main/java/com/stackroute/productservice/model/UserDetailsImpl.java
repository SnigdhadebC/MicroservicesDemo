package com.stackroute.productservice.model;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.*;

/**
 * UserDetailsImpl class represents the UserDetails Object which will
 * be returned by UserDetailsService after authentication
 * <p>
 * ** TODO **
 * Complete the class using the unit tests given.
 * This class should implement UserDetails interface of Spring Security
 * and override all the methods of the interface
 */

public class UserDetailsImpl implements UserDetails {

    private String username;
    private String password;
    private boolean active;
    private List<GrantedAuthority> authorities;

    public UserDetailsImpl() {

    }

    /**
     * Parameterized constructor should accept a User object
     * UserDetailsImpl properties should be initialized using the user object passed as parameter
     * ** TODO **
     * Complete the constructor
     */

    public UserDetailsImpl(User user) {
        this.username = user.getUsername();
        this.password = user.getPassword();
        this.active = user.isActive();
        this.authorities = Arrays.asList(new SimpleGrantedAuthority(user.getRole()));
     }

    /**
     * **TODO**
     * Methods of UserDetailsService to be overridden here
     */

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return authorities;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public boolean isAccountNonExpired() {
        return true;
    }

    @Override
    public boolean isAccountNonLocked() {
        return true;
    }

    @Override
    public boolean isCredentialsNonExpired() {
        return true;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }
}
