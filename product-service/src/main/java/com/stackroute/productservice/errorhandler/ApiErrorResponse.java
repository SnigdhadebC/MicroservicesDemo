package com.stackroute.productservice.errorhandler;

import lombok.Builder;
import lombok.Data;
import org.springframework.http.HttpStatus;

import java.util.ArrayList;
import java.util.List;

/**
 * ApiErrorResponse represent the response sent to the client by the APiExceptionHandler
 */
@Data
@Builder
public class ApiErrorResponse {
    private int statusCode;
    private HttpStatus status;
    private String message;
    @Builder.Default
    private List<String> errors = new ArrayList<>();
}
