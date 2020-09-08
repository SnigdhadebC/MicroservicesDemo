package com.stackroute.productservice.controller;

import com.stackroute.productservice.dto.ProductDto;
import com.stackroute.productservice.errorhandler.exception.ProductExistsException;
import com.stackroute.productservice.errorhandler.exception.ProductNotFoundException;
import com.stackroute.productservice.model.Product;
import com.stackroute.productservice.service.ProductService;
import org.modelmapper.ModelMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.util.List;
import java.util.Map;

/**
 * This controller provides a web interface for users having admin access to manage products
 */

@RequestMapping("/admin")
@Controller
public class ProductAdminMvcController {

    public static final String PRODUCTS_ADMINISTRATION_VIEWNAME = "products_administration";
    public static final String REDIRECT_ADMIN_DASHBOARD = "redirect:/admin/products";
    private final Logger logger = LoggerFactory.getLogger(ProductAdminMvcController.class);

    private final ProductService productService;
    private final ModelMapper modelMapper;


    @Autowired
    public ProductAdminMvcController(ProductService productService, ModelMapper modelMapper) {
        this.productService = productService;
        this.modelMapper = modelMapper;
    }

    /**
     * Gets the Products by category
     */
    @GetMapping({"", "/products"})
    public String getProductsByCategory(@RequestParam(defaultValue = "Fruits") String category, Model model) {
        List<Product> products = productService.fetchProductsByCategory(category);

        model.addAttribute("products", products);
        model.addAttribute("category", category.toUpperCase());
        Map<String,Object> redirectAttributes = model.asMap();
        Object message = redirectAttributes.get("message");
        if (message != null) {
            model.addAttribute("message", (String) message);
        }
        Object product = redirectAttributes.get("editProduct");
        if(product != null){
            model.addAttribute("editProduct", (ProductDto)product);
        }
        return PRODUCTS_ADMINISTRATION_VIEWNAME;
    }

    /**
     * Adds new Product
     */
    @PostMapping("/products/add")
    public String addNewProduct(ProductDto productDto, RedirectAttributes redirectAttributes) {
        logger.info("Adding Product : {}", productDto);
        try {
            productService.addNewProduct(convertToEntity(productDto));
            redirectAttributes.addFlashAttribute("message", "Product successfully added");

        } catch (ProductExistsException ex) {
            String message = "Product with id " + productDto.getId() + " already exists";
            redirectAttributes.addFlashAttribute("message", message);
            redirectAttributes.addFlashAttribute("editProduct", productDto);
            logger.error(message);
        }
        return REDIRECT_ADMIN_DASHBOARD + "?category=" + productDto.getCategory();
    }

    /**
     * Adds new Product
     */
    @GetMapping("/products/delete")
    public String deleteProduct(@RequestParam String productId, @RequestParam String category, RedirectAttributes redirectAttributes) {
        logger.info("Deleting Product : {}", productId);
        Product deletedProduct = null;
        try {
            deletedProduct = productService.removeProductById(productId);
            redirectAttributes.addFlashAttribute("message", "Product deleted");

        } catch (ProductNotFoundException ex) {
            String message = "Product with id " + productId + " does not exist";
            logger.error(message);
        }
        return REDIRECT_ADMIN_DASHBOARD + "?category=" + category;
    }


    private Product convertToEntity(ProductDto productDto) {
        return modelMapper.map(productDto, Product.class);
    }
}
