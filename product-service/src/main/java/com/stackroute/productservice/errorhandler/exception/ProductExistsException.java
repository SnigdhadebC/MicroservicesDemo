package com.stackroute.productservice.errorhandler.exception;

public class ProductExistsException extends Exception {
    public ProductExistsException() {
        super();
    }

    public ProductExistsException(String message) {
        super(message);
    }
}
