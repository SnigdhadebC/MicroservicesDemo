package com.stackroute.productservice.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

/**
 * This class is to provide the security configuration for the web application
 * which will be used by Admins to manage products
 * <p>
 * **TODO**
 * Add appropriate annotation to this class for creation of Beans
 * Implement interface WebSecurityConfigurerAdapter
 */
@Configuration
public class SecurityConfiguration extends WebSecurityConfigurerAdapter {

    /**
     * TODO**
     * Inject UserDetailsService
     */
    @Autowired
    private UserDetailsService userDetailsService;

    /**
     * TODO**
     * Override method configure(AuthenticationManagerBuilder auth) and
     * configure the userDetailsService and passwordEncoder
     */
    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        auth.userDetailsService(userDetailsService).passwordEncoder(passwordEncoder());
    }
    /**
     * TODO**
     * Create a PasswordEncoder bean for encrypting using BCrypt
     */
    @Bean
    public PasswordEncoder passwordEncoder(){
        return new BCryptPasswordEncoder();
    }

    /**
     * TODO**
     * Override method configure(HttpSecurity httpSecurity) to achieve the following
     * Any requests to the Rest endpoints of product service should be allowed without authentication
     * Any request for using web application by Admin should be authenticated
     * Use Spring's Default login for Form login
     * login processing url should be /admin/processlogin
     * After successful login, the page should be redirected to default url /admin
     * logout should be allowed for all.
     * logout url should be configured as /admin/logout
     */
    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.csrf().disable()
                 .authorizeRequests()
                    .antMatchers("/api/v1/products").permitAll()
                    .antMatchers("/admin").authenticated()
                .and()
                    .formLogin()
                    .loginProcessingUrl("/admin/processlogin")
                    .successForwardUrl("/admin")
                .and()
                    .logout()
                    .invalidateHttpSession(true);
    }
    /**
     * TODO**
     * Override method configure(WebSecurity web) to ignore d
     * the paths of used by Swagger api like /swagger-resources/**, etc
     */

    @Override
    public void configure(WebSecurity web) throws Exception {
        web.ignoring().antMatchers("/v2/api-docs",
                "/configuration/ui",
                "/swagger-resources/**",
                "/configuration/security",
                "/swagger-ui.html",
                "/webjars/**");
    }
}
