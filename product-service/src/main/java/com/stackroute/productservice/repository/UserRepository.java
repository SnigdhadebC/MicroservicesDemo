package com.stackroute.productservice.repository;

import com.stackroute.productservice.model.User;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * Repository for getting user details for Admin from H2 database
 */
public interface UserRepository extends JpaRepository<User, Integer> {
    Optional<User> findUserByUsername(String username);

    boolean existsByUsername(String username);
}
