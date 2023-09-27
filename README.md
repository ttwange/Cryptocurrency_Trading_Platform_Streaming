# Cryptocurrency_Trading_Platform_Streaming
A kafka architecture for an api

![kafka architecture (1)](https://github.com/ttwange/Cryptocurrency_Trading_Platform_Streaming/assets/86237194/f82579b4-3e37-4d0d-8bd6-8c19f493850f)


# High-Transaction Financial API Data Processing

## Table of Contents
- [Why This Project?](#why-this-project)
- [Scenarios](#scenarios)
  - [Scenario: Real-Time Cryptocurrency Trading Platform](#scenario-real-time-cryptocurrency-trading-platform)
- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Implementation](#implementation)
  - [Kafka to MongoDB Consumer](#kafka-to-mongodb-consumer)
  - [Kafka to PostgreSQL Consumer](#kafka-to-postgresql-consumer)
- [Deployment Plan](#deployment-plan)
- [Usage](#usage)
- [Configuration](#configuration)
- [Maintenance](#maintenance)

## Why This Project?

In this case study, we simulate a high-transaction financial company operating a real-time cryptocurrency trading platform. Users buy and sell cryptocurrencies, and the platform needs to provide up-to-the-second data on cryptocurrency prices, market orders, and user transactions. Here's why we use Kafka to feed data into MongoDB and PostgreSQL, also have the whole project run on docker 

![docker](https://github.com/ttwange/Cryptocurrency_Trading_Platform_Streaming/assets/86237194/bf714816-ca11-4330-9336-bfffdd168a4b)



### Scenario: Real-Time Cryptocurrency Trading Platform

In this scenario, the company operates a real-time cryptocurrency trading platform. Users buy and sell cryptocurrencies, and the platform needs to provide up-to-the-second data on cryptocurrency prices, market orders, and user transactions. Here's why we might use Kafka to feed data into MongoDB and PostgreSQL:

1. **Data Ingestion and Real-Time Processing**: The Crypto API provides a continuous stream of data with real-time cryptocurrency price updates and market order information. Kafka acts as a high-throughput, real-time data ingestion system that can handle this continuous stream efficiently. The data is pushed into Kafka topics as it arrives, ensuring that no data is lost due to spikes in the API traffic.

2. **MongoDB for Fast, Flexible Storage**: MongoDB is a NoSQL database known for its flexibility in handling unstructured and semi-structured data. In this scenario, MongoDB can be used to store real-time user transaction data. Each user's trading history, wallet balances, and order information can be stored as documents in MongoDB, providing fast read and write access to user-specific data. This allows users to quickly view their trading history and account balances.

3. **PostgreSQL for Structured Data and Analytics**: PostgreSQL is a powerful relational database known for its robust SQL capabilities. It is well-suited for structured financial data and complex queries. In this case, PostgreSQL is used to store historical cryptocurrency price data, order book snapshots, and other structured data. It also serves as the backend for analytics tools used by traders and analysts to study historical price trends, perform technical analysis, and make informed trading decisions.

4. **Data Consistency**: By having separate databases for user transactions (MongoDB) and historical market data (PostgreSQL), this ensures data consistency and separation of concerns. This separation allows for better management, scalability, and optimization of each database based on its specific use case.

5. **Scaling and Load Balancing**: Since kafka enables horizontal scaling, allowing multiple instances of consumers for MongoDB and PostgreSQL. This helps distribute the workload and ensures that the databases can handle high traffic and data volumes without becoming bottlenecks in the system.

6. **Data Redundancy and Backup**: Storing data in both MongoDB and PostgreSQL provides redundancy and backup capabilities. If one database experiences issues, data can still be retrieved from the other, minimizing downtime and ensuring data integrity.

7. **Transaction History**: MongoDB's document-oriented structure is well-suited for storing individual user transaction histories. This allows users to retrieve their transaction details quickly, which is crucial for traders who need to review their past trades for tax reporting or auditing purposes.

## Project Overview

Our project simulates a high-transaction financial company that relies on a cryptocurrency API as a data source. The primary goal is to ingest real-time data from this API, transform it, and distribute it to two different data stores: MongoDB and PostgreSQL. This architecture is designed to ensure data reliability, scalability, and availability, making it suitable for mission-critical financial operations.

## Architecture

The project architecture is centered around a series of well-defined steps:

1. **API Data Ingestion**: Real-time data is pulled from the cryptocurrency API.

2. **Data Transformation**: The raw API data is cleaned and transformed into a structured format for further processing.

3. **Data Distribution via Kafka**: The transformed data is distributed to a Kafka topic, acting as an intermediary.

4. **Data Consumption**: Two consumers, one for MongoDB and one for PostgreSQL, retrieve and store the data in their respective databases.

## Implementation

### Kafka to MongoDB Consumer

We have implemented a Python script that consumes data from the Kafka topic and inserts it into MongoDB. This ensures data availability in a flexible, schema-less format, ideal for accommodating various cryptocurrency attributes.

![mongodb](https://github.com/ttwange/Cryptocurrency_Trading_Platform_Streaming/assets/86237194/09075003-bf82-454e-958f-f4bab58267a3)


### Kafka to PostgreSQL Consumer

A separate Python script consumes data from the same Kafka topic and inserts it into PostgreSQL. This relational database is well-suited for structured financial data, allowing for complex queries and analysis.

![post](https://github.com/ttwange/Cryptocurrency_Trading_Platform_Streaming/assets/86237194/7d5eac35-8a20-438d-aaea-0627a2e7fcb1)


## Deployment Plan

A Prefect Deployment Plan has been created to automate the ETL (Extract, Transform, Load) process. The 'extract-Load-transform' flow is scheduled to run every minute, simulating real-time data updates and ensuring data is up-to-date.

## Usage

To maintain real-time data processing, the deployment plan runs the ETL flow every minute. The ETL process handles data ingestion, transformation, and distribution to both MongoDB and PostgreSQL.

## Configuration

- Customize configuration parameters
To get to a working I had a struggle of 65 failure before getting the first one to work

![dashboard](https://github.com/ttwange/Cryptocurrency_Trading_Platform_Streaming/assets/86237194/2438f100-9989-4c52-a951-87bb758c14df)

