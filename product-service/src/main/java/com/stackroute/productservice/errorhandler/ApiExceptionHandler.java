package com.stackroute.productservice.errorhandler;

import com.stackroute.productservice.errorhandler.exception.ProductExistsException;
import com.stackroute.productservice.errorhandler.exception.ProductNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.ObjectError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * This class handles exceptions occurring in the API and sends an error response
 */

@ControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {
    /**
     * **TODO**
     * Create a slf4j Logger for logging messages to standard output
     */
    private Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    /**
     * Exception handler for ProductExistsException
     * **TODO**
     * Log the below `error` message in the method
     *  "Building error response for ProductNotFoundException"
     */
    @ExceptionHandler(ProductNotFoundException.class)
    public ResponseEntity<ApiErrorResponse> handleProductNotFoundException(ProductNotFoundException exception, WebRequest request) {
        ApiErrorResponse errorResponse = buildErrorResponse(exception.getLocalizedMessage(), HttpStatus.NOT_FOUND);
        log.error("Building error response for ProductNotFoundException");
        return new ResponseEntity<>(errorResponse, errorResponse.getStatus());
    }

    /**
     * Exception handler for ProductExistsException
     * **TODO**
     * Log the below `error` message in the method
     *  "Building error response for ProductExistsException"
     */
    @ExceptionHandler(ProductExistsException.class)
    public ResponseEntity<ApiErrorResponse> handleProductExistsException(ProductExistsException exception, WebRequest request) {
        ApiErrorResponse errorResponse = buildErrorResponse(exception.getLocalizedMessage(), HttpStatus.CONFLICT);
        log.error("Building error response for ProductExistsException");
        return new ResponseEntity<>(errorResponse, errorResponse.getStatus());
    }

    /**
     * Exception handler for Invalid data in Http Request
     * **TODO**
     * Log the below `error` message in the method
     *  "Building error response for invalid request parameters"
     */
    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(MethodArgumentNotValidException exception, HttpHeaders headers, HttpStatus status, WebRequest request) {

        List<String> validationMessages = new ArrayList<>();
        for (ObjectError error : exception.getBindingResult().getAllErrors()) {
            validationMessages.add(error.getDefaultMessage());
        }
        String message = "Validation of Request failed";
        ApiErrorResponse errorResponse = buildErrorResponse(message, HttpStatus.BAD_REQUEST);
        errorResponse.getErrors().addAll(validationMessages);
        log.error("Building error response for invalid request parameters");
        return handleExceptionInternal(exception, errorResponse, headers, errorResponse.getStatus(), request);
    }

    /**
     * Exception handler for exceptions other than the above
     * **TODO**
     * Log the below `error` message in the method
     *  "Building error response for exceptions"
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiErrorResponse> handleAllException(Exception exception, WebRequest request) {
        ApiErrorResponse errorResponse = buildErrorResponse(exception.getLocalizedMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        log.error("Building error response for exceptions");
        return new ResponseEntity<>(errorResponse, errorResponse.getStatus());
    }

    /**
     * Building standard error response for the exceptions in the API
     */
    private ApiErrorResponse buildErrorResponse(String message, HttpStatus httpStatus) {
        return ApiErrorResponse.builder()
                .status(httpStatus)
                .statusCode(httpStatus.value())
                .message(message)
                .build();

    }
}