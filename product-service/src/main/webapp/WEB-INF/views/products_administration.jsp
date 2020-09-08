<!doctype html>
<%@ page language="java"
contentType="text/html; charset=ISO-8859-1"
pageEncoding="ISO-8859-1"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>

<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>OrganicWorld</title>
    <link crossorigin="anonymous" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css"
          integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
    <link href="/admin/styles.css" rel="stylesheet">
</head>
<body>
<nav class="navbar navbar-expand-lg navbar-dark bg-dark">
    <a class="navbar-brand" href="#">OrganicWorld</a>
    <button aria-controls="navbarNavAltMarkup" aria-expanded="false" aria-label="Toggle navigation"
            class="navbar-toggler"
            data-target="#navbarNavAltMarkup" data-toggle="collapse" type="button">
        <span class="navbar-toggler-icon"></span>
    </button>
    <div class="collapse navbar-collapse" id="navbarNavAltMarkup">
        <div class="navbar-nav">
            <a class="nav-item nav-link active" href="/admin/products?category=Fruits">Fruits <span class="sr-only">(current)</span></a>
        </div>
        <div class="navbar-nav">
            <a class="nav-item nav-link active" href="/admin/products?category=Vegetables">Vegetables <span
                    class="sr-only">(current)</span></a>
        </div>
        <div class="navbar-nav">
            <a class="nav-item nav-link active" href="/admin/products?category=Cereals">Cereals <span
                    class="sr-only">(current)</span></a>
        </div>
        <div class="ml-3 p-2">
            <form class="form-inline" action="/admin/products">
                <input name="category" class="form-control mr-sm-2" type="search" placeholder="Enter Category" aria-label="Search">
                <button class="btn btn-outline-success my-2 my-sm-0" type="submit">Search</button>
            </form>
        </div>
        <div class="navbar-nav ml-5">
            <a class="nav-item nav-link active btn btn-success" href="#add_new_product">Add Product<span
                    class="sr-only">(current)</span></a>
        </div>
        <form class="form-inline my-2 my-lg-0 ml-auto">
            <button class="btn btn-outline-danger my-2 my-sm-0" formAction="/admin/logout" type="submit">Logout</button>
        </form>
    </div>



</nav>

<section class="product-table">
    <p class="pt-2 text-primary"><strong>${message}</strong></p>
    <hr>
    <h2>${category} </h2>

    <c:if test="${ empty products}">
        <h3>No products in given category</h3>
    </c:if>

    <c:if test="${not empty products}">
        <table aria-describedby="products" class="table table-striped">
            <thead>
            <tr>
                <th scope="col">ID</th>
                <th scope="col">Name</th>
                <th scope="col">Price</th>
                <th scope="col">Description</th>
                <th scope="col">Action</th>
            </tr>
            </thead>
            <tbody>
            <c:forEach items="${products}" var="product">
                <tr>
                    <th scope="row">${product.id}</th>
                    <td>${product.name}</td>
                    <td>${product.price}</td>
                    <td>${product.description}</td>
                    <td><a href="/admin/products/delete?productId=${product.id}&category=${product.category}"><i class="fa fa-trash ml-3"></i></a></td>
                </tr>
            </c:forEach>
            </tbody>
        </table>
    </c:if>

</section>
<hr>
<section id="add_new_product" class="add-product">
    <hr>
    <h5 class="mx-auto">Add New Product</h5>
    <hr>
    <form action="/admin/products/add" method="post">
        <div class="form-row">
            <div class="form-group col-md-3">
                <label>Category</label>
                <select class="form-control" name="category">
                    <option value="Fruits" ${editProduct.category=='Fruits' ? 'selected' : ''}>Fruits</option>
                    <option value="Vegetables" ${editProduct.category=='Vegetables' ? 'selected' : ''}>Vegetables</option>
                    <option value="Cereals" ${editProduct.category=='Cereals' ? 'selected' : ''}>Cereals</option>

                </select>
            </div>

            <div class="form-group col-md-3">
                <label>Product Id</label>
                <input class="form-control" name="id" placeholder="Product Id" required type="text" value="${editProduct.id}">
            </div>
            <div class="form-group col-md-4">
                <label>Product Name</label>
                <input class="form-control" name="name" placeholder="Name of the product" required type="text" value="${editProduct.name}">
            </div>
            <div class="form-group col-md-2">
                <label>Price</label>
                <input class="form-control" min="0" name="price" placeholder="Price" required type="number" value="${editProduct.price}">
            </div>
        </div>
        <div class="form-group">
            <label>Description</label>
            <textarea class="form-control" name="description" required >${editProduct.description}</textarea>
        </div>

        <div class="form-group">
            <label>Product Image</label>
            <input class="form-control" name="imageUrl" placeholder="Image url of product" required type="text" value="${editProduct.imageUrl}">
        </div>
        <button class="btn btn-primary" type="submit">Add</button>
    </form>
</section>

<script crossorigin="anonymous"
        integrity="sha384-KJ3o2DKtIkvYIK3UENzmM7KCkRr/rE9/Qpg6aAZGJwFDMVNA/GpGFF93hXpG5KkN"
        src="https://code.jquery.com/jquery-3.2.1.slim.min.js"></script>
<script crossorigin="anonymous"
        integrity="sha384-ApNbghB+Y1QKtv3Rn7W3mgPxhU9K/ScQsAP7hUibX39j7fakFPskvXusvfa0b4Q"
        src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.12.9/umd/popper.min.js"></script>
<script crossorigin="anonymous"
        integrity="sha384-JZR6Spejh4U02d8jOt6vLEHfe/JQGiRRSQQxSfFWpi1MquVdAyjUar5+76PVCmYl"
        src="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/js/bootstrap.min.js"></script>
</body>
</html>