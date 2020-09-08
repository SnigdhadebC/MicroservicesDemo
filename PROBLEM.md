# Organic World Case study - Implementing Microservices patterns

## Problem Statement

Organic World has started business in 2017 as an organic food store. It started in a 100 sq. ft. store at Detroit. 
Due to its high quality products, reasonable price and steady supply, the store has gained popularity and currently
operates with twenty outlets in Detroit. Organic World has plans to open multiple stores across the country. 


To cater to its growing business, Organic World wants to digitize their operation. The objective of Organic World is 
to create a brand for itself and increase their customer volumes.

## Proposed Solution
In order to increase their consumers, Organic World needs a solution that will help to reach out to users over the web
 for selling their products online.
Organic World has contracted the job to build the software to Tech Connoisseurs Ltd – a software development company.
The Company have already developed and demonstrated the front-end of the product to Organic World satisfactorily.

Because of the evolving business requirements of Organic World, Tech Connoisseurs Ltd have proposed to develop the 
application backend based on the Microservice's architecture, deployable on cloud

## High Level Requirements

The high level requirements for the initial release of application backend are given below:

 - Create the User service for Customer registration and authentication
 - Implement asynchronous messaging in User service to consume order details from Kafka
 - Create the Product service with REST endpoints for searching products
 - Implement Spring Web MVC in Product service to allow Admins to manage products. Web Application should be secured using Spring security
 - Create the Order service and implement asynchronous messaging to produce order details message to Kafka
 - Implement Caching for faster search of products
 - Implement Logging
 - Implement Configuration Server
 - Implement Service Registry
 - Implement Routing in API Gateway/Gateway Aggregation/Gateway Offloading


**There would be two roles in this application. They would be allowed to perform activities based on their role:** 
  - **Customer**: Search the products based on a category and place the order
  - **Admin**: Manage the products

## Microservices

**Base codes for the below microservices has been already provided** 

- **user-service**: Responsible for managing customer profiles and authentication of customers 
- **product-service**: Responsible for searching products and providing a web interface to admin for managing products 
- **order-service**: Responsible for placing orders for products 

- **configuration-service**: Responsible for working as a centralized configuration server 
- **service-registry**: Responsible for registration and discovery of microservice instances
- **api-gateway-service**: Responsible for working as a gateway for all requests for all microservices and to authenticate the user(if required)

## The following tasks needs to be completed as part of this case study. 
    
    Respective classes contain the **TODO** comments for the tasks to be completed

 - Step 1 : Add API gateway to route requests and to configure CORS for all microservices
 - Step 2 : Modify the User service to consume order details message from Kafka
 - Step 3 : Modify the Order service to post order details message to Kafka
 - Step 4 : Modify the Product service to add spring security for admin authentication
 - Step 5 : Implement caching in Product Service using Redis
 - Step 6 : Implement logging in Microservices

## Tech Stack

    - Java 11
    - Spring Boot 2.x
    - Spring Framework 5
    - REST 2.0 API
    - MySQL v8
    - MongoDB 4.x
    - Netflix Eureka
    - Netflix Zuul
    - Spring cloud config
    - JUnit 5 and Mockito
    - Kafka
    - JWT
    - slf4j

### Important Information

#### Running the application locally after cloning
    > After implementing the requirements, execute the following maven command in the parent/root project

            mvn clean package
    
    > Start Redis, MySQL, Kafka, MongoDb. Instruction for installation and starting these are provided in below section

    > The Services have to be started in the following order
        - configuration-service
        - service-registry
        - api-gateway-service
        
        followed by the remaining services. 
    
     > Use the following maven command to start individual services from their respective folders
        
            mvn spring-boot:run
    
    Swagger Documentation for the microservies can be accessed at below urls:
     
        - http://localhost:8765/order-service/swagger-ui.html
        - http://localhost:8765/product-service/swagger-ui.html
        - http://localhost:8765/user-service/swagger-ui.html 
     
    Admin Dashboard can be accessed at below url 
     
        - http://localhost:9000/admin 
          (Default username: admin password: admin)
   
#### Following software needs to be available/installed in the local environment

**MongoDb**

    > Use the following commands on ubuntu terminal for installing MongoDb
            
        sudo apt update
        sudo apt install -y mongodb
            
    > Check the Server status using following command
                    
        sudo systemctl status mongodb
        
    > Use the following command to connect to MongoDb server using mongo cli
                
            mongo
            
**Kafka**

     > Refer the steps provided at the below url to install and start apache kafka
        
         https://kafka.apache.org/quickstart
    
**Redis Server**

      > Use the following commands on ubuntu terminal for installing Redis Server

            sudo apt update
            sudo apt install redis-server
        
      > Check the Server status using following command
            
            sudo systemctl status redis-server
        
      > Use the following command to connect to Redis server using redis cli
        
            redis-cli 
            
**MySQL Server**
      
      > Check whether Mysql is installed in the environment provided to you by running the below command in terminal
             
             mysql --version
      
      > If MySQL is not installed, Refer the steps provided at the below url to install and start MySQL Server.
        
            https://linuxize.com/post/how-to-install-mysql-on-ubuntu-18-04/