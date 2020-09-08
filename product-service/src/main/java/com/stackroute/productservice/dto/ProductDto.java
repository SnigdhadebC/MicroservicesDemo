package com.stackroute.productservice.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotEmpty;

/**
 * Represents the Data Transfer object for Product Entity
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductDto {

    @NotEmpty(message = "{product.id.empty}")
    private String id;

    @NotEmpty(message = "{product.name.empty}")
    private String name;
    private String description;

    @NotEmpty(message = "{product.category.empty}")
    private String category;
    private double price;

    @JsonProperty("image")
    private String imageUrl;
}
