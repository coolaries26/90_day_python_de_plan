# 🛒 E-Commerce Analytics & Machine Learning Platform

### Executive Data Engineering Capstone | End-to-End Analytics, Automation & AI Platform

---

## 📖 Executive Summary

This project demonstrates how a modern e-commerce organization can transform raw transactional data into actionable business intelligence, predictive insights, and automated decision-making.

Using **100,000+ real-world orders** from the Brazilian e-commerce marketplace **Olist**, this platform showcases a complete enterprise-grade data lifecycle:

✅ Data Ingestion
✅ Data Warehousing
✅ Data Transformation & Analytics
✅ Machine Learning Predictions
✅ Workflow Automation
✅ Executive Dashboards & Reporting

The solution mirrors the architecture used by leading e-commerce companies such as Amazon, Shopify, Mercado Libre, Flipkart, and Zalando to monitor operations, improve customer retention, optimize logistics, and drive revenue growth.

---

# 🎯 Business Objectives

The platform is designed to answer critical business questions:

### Customer Analytics

* Who are our most valuable customers?
* Which customers are likely to stop purchasing?
* What is the lifetime value (LTV) of each customer?

### Order & Logistics Analytics

* What percentage of orders are delivered late?
* Which regions experience the highest delays?
* How does delivery performance impact customer satisfaction?

### Seller Performance Analytics

* Which sellers generate the highest revenue?
* Which sellers have poor customer ratings?
* How can underperforming sellers be identified proactively?

### Product Intelligence

* Which product categories generate the most revenue?
* Which products contribute to customer dissatisfaction?
* What are the seasonal buying trends?

### Predictive Analytics

* Will a customer place another order?
* Is an order likely to be delivered late?
* Which customers are at risk of churn?

---

# 📊 Dataset Overview

## Brazilian E-Commerce Dataset (Olist)

### About the Dataset

The project uses the publicly available **Olist Brazilian E-Commerce Dataset**, one of the most widely used datasets for Data Engineering, Analytics, and Machine Learning portfolios.

**Source:**
[Kaggle - Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?utm_source=chatgpt.com)

### Dataset Statistics

| Metric            | Value       |
| ----------------- | ----------- |
| Total Orders      | 99,441+     |
| Customers         | 96,000+     |
| Sellers           | 3,000+      |
| Products          | 32,000+     |
| Time Period       | 2016 – 2018 |
| Source Files      | 8+          |
| Records Processed | 100,000+    |

---

## Dataset Components

| Table                               | Purpose                                        |
| ----------------------------------- | ---------------------------------------------- |
| `olist_orders_dataset`              | Order status and lifecycle timestamps          |
| `olist_order_items_dataset`         | Products purchased in each order               |
| `olist_order_payments_dataset`      | Payment methods and payment amounts            |
| `olist_order_reviews_dataset`       | Customer ratings and reviews                   |
| `olist_customers_dataset`           | Customer demographic and location information  |
| `olist_sellers_dataset`             | Seller information and geographic distribution |
| `olist_products_dataset`            | Product metadata and dimensions                |
| `product_category_name_translation` | Portuguese-to-English category mapping         |

---

# ⭐ Why This Project Matters

This project demonstrates the exact combination of skills organizations seek when hiring:

### Data Engineering

* ETL Development
* Data Warehousing
* Data Modeling
* SQL Optimization

### Analytics Engineering

* KPI Development
* Business Metrics
* Reporting Automation

### Machine Learning

* Customer Churn Prediction
* Delivery Delay Prediction
* Model Retraining Pipelines

### Platform Engineering

* Workflow Orchestration
* CI/CD Concepts
* Production Scheduling

### Business Intelligence

* Executive Dashboards
* Operational Reporting
* Data Storytelling

---

# 🏗️ Solution Architecture

```text
                    ┌────────────────────────────────────┐
                    │     E-COMMERCE DATA PLATFORM       │
                    └────────────────────────────────────┘

          Raw CSV Files (Kaggle / Olist Dataset)
                            │
                            ▼
                    ┌─────────────────┐
                    │ Data Ingestion  │
                    └─────────────────┘
                            │
                            ▼
                    PostgreSQL Data Warehouse
                            │
        ┌───────────────────┴───────────────────┐
        │                                       │
        ▼                                       ▼
   raw schema                          analytics schema
   (source data)                      (business metrics)
        │                                       │
        ▼                                       ▼
 Customer Analytics                  Seller Analytics
 Product Analytics                   Revenue Analytics
 Order Analytics                     KPI Calculations
        │
        ▼
                 Machine Learning Layer
        ┌───────────────────────────────────────┐
        │ Customer Churn Prediction Model       │
        │ Delivery Delay Prediction Model       │
        └───────────────────────────────────────┘
                            │
                            ▼
                    Airflow Orchestration
        ┌───────────────────────────────────────┐
        │ Daily ETL Refresh                     │
        │ Weekly ML Retraining                  │
        │ Dashboard Data Refresh                │
        └───────────────────────────────────────┘
                            │
                            ▼
                    Streamlit Dashboard
```

---

# 🗄️ Data Warehouse Design

## Raw Layer

The Raw Layer stores source data exactly as received from the CSV files.

```text
raw.*
├── customers
├── orders
├── order_items
├── order_payments
├── order_reviews
├── products
├── sellers
└── category_translation
```

Purpose:

* Preserve source integrity
* Enable auditability
* Support data lineage tracking

---

## Analytics Layer

The Analytics Layer contains transformed and business-ready datasets.

### Customer Lifetime Value

```text
analytics.customer_ltv
```

Business Value:

* Customer segmentation
* Loyalty analysis
* Marketing targeting

---

### Order Metrics

```text
analytics.order_metrics
```

Business Value:

* Delivery performance
* Customer experience measurement
* SLA monitoring

---

### Seller Performance

```text
analytics.seller_performance
```

Business Value:

* Revenue contribution analysis
* Seller ranking
* Quality assessment

---

### Product Analytics

```text
analytics.product_analytics
```

Business Value:

* Product profitability
* Category performance
* Product trend analysis

---

### Monthly Revenue

```text
analytics.monthly_revenue
```

Business Value:

* Revenue forecasting
* Executive reporting
* Trend analysis

---

# 🤖 Machine Learning Components

## Customer Churn Prediction

### Objective

Predict whether a customer is likely to make another purchase.

### Business Impact

* Reduce customer attrition
* Improve retention campaigns
* Increase repeat purchases

### Output

```text
churn_model.pkl
```

---

## Delivery Delay Prediction

### Objective

Predict whether an order is likely to be delivered late.

### Business Impact

* Improve customer satisfaction
* Reduce support costs
* Enhance logistics planning

### Output

```text
delay_model.pkl
```

---

# 🔄 Workflow Automation (Apache Airflow)

The platform uses Apache Airflow to automate recurring business processes.

## Daily ETL Pipeline

```text
dag_ecommerce_etl
```

Tasks:

* Load new data
* Execute transformations
* Refresh analytics tables

---

## Weekly Model Retraining

```text
dag_ml_retrain
```

Tasks:

* Retrain prediction models
* Evaluate model performance
* Publish updated models

---

## Dashboard Refresh

```text
dag_dashboard_refresh
```

Tasks:

* Refresh KPIs
* Update charts
* Publish latest insights

---

# 📈 Executive Dashboard

Built using Streamlit.

Provides a single-pane view of operational and business performance.

### Executive KPIs

* Revenue
* Orders
* Customers
* Sellers
* Average Order Value
* Customer Satisfaction Score

### Analytics Views

#### Overview

Enterprise performance summary

#### Orders

Order trends and delivery performance

#### Customers

Customer segmentation and LTV

#### Sellers

Seller rankings and performance metrics

#### Products

Category performance and revenue analysis

#### AI Insights

* Churn Risk Scores
* Delay Predictions
* Predictive Business Trends

---

# 🎯 Portfolio & Interview Value

This project demonstrates the complete lifecycle of modern data platforms used by:

* Amazon
* Shopify
* Mercado Libre
* Flipkart
* Zalando
* Walmart Global Tech
* Target
* Enterprise Retail Organizations

A candidate can confidently discuss:

> “I designed and implemented an end-to-end analytics platform that ingests 100,000+ e-commerce transactions into PostgreSQL, transforms data into business-ready analytics models, orchestrates workflows using Airflow, builds machine learning models for churn and delivery prediction, and serves insights through an executive dashboard.”

---

# 🚀 Quick Start

## Prerequisites

### Software

* Python 3.12+
* PostgreSQL 17+
* Git
* Virtual Environment

### Configuration

Configure:

```text
.env
```

using:

```text
.env.example
```

---

## Launch Dashboard

```bash
cd C:\90_day_python_de_plan
capstone\dashboard\run_dashboard.bat
```

Dashboard URL:

```text
http://localhost:8502
```

---

# 🏆 Outcome

By the end of this project, stakeholders gain:

* A centralized analytics platform
* Automated reporting capabilities
* Predictive business intelligence
* Improved operational visibility
* Scalable data engineering architecture
* A foundation for future AI-driven decision making

This project serves as a blueprint for building enterprise-grade data platforms that combine **Data Engineering, Analytics, Machine Learning, Automation, and Executive Reporting** into a single integrated solution.
