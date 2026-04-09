# DVD Rental Schema Report
Generated: 2026-04-10 00:26:37

## Tables

| Table | Rows | Columns | Size |
|-------|------|---------|------|
| rental | 16,044 | 7 | 2352 kB |
| payment | 14,596 | 6 | 1816 kB |
| film | 1,000 | 13 | 936 kB |
| film_actor | 5,462 | 3 | 488 kB |
| inventory | 4,581 | 4 | 440 kB |
| customer | 599 | 10 | 208 kB |
| address | 603 | 8 | 152 kB |
| film_category | 1,000 | 3 | 112 kB |
| city | 600 | 4 | 112 kB |
| actor | 200 | 4 | 72 kB |
| store | 2 | 4 | 40 kB |
| staff | 2 | 11 | 32 kB |
| category | 16 | 3 | 24 kB |
| country | 109 | 3 | 24 kB |
| language | 6 | 3 | 24 kB |

## Column Details

### rental

| # | Column | Type | Nullable | PK |
|---|--------|------|----------|----|
| 1 | rental_id | integer | NO | YES |
| 2 | rental_date | timestamp without time zone | NO | NO |
| 3 | inventory_id | integer | NO | NO |
| 4 | customer_id | smallint | NO | NO |
| 5 | return_date | timestamp without time zone | YES | NO |
| 6 | staff_id | smallint | NO | NO |
| 7 | last_update | timestamp without time zone | NO | NO |

### payment

| # | Column | Type | Nullable | PK |
|---|--------|------|----------|----|
| 1 | payment_id | integer | NO | YES |
| 2 | customer_id | smallint | NO | NO |
| 3 | staff_id | smallint | NO | NO |
| 4 | rental_id | integer | NO | NO |
| 5 | amount | numeric | NO | NO |
| 6 | payment_date | timestamp without time zone | NO | NO |

### film

| # | Column | Type | Nullable | PK |
|---|--------|------|----------|----|
| 1 | film_id | integer | NO | YES |
| 2 | title | character varying | NO | NO |
| 3 | description | text | YES | NO |
| 4 | release_year | integer | YES | NO |
| 5 | language_id | smallint | NO | NO |
| 6 | rental_duration | smallint | NO | NO |
| 7 | rental_rate | numeric | NO | NO |
| 8 | length | smallint | YES | NO |
| 9 | replacement_cost | numeric | NO | NO |
| 10 | rating | USER-DEFINED | YES | NO |
| 11 | last_update | timestamp without time zone | NO | NO |
| 12 | special_features | ARRAY | YES | NO |
| 13 | fulltext | tsvector | NO | NO |

### film_actor

| # | Column | Type | Nullable | PK |
|---|--------|------|----------|----|
| 1 | actor_id | smallint | NO | YES |
| 2 | film_id | smallint | NO | YES |
| 3 | last_update | timestamp without time zone | NO | NO |

### inventory

| # | Column | Type | Nullable | PK |
|---|--------|------|----------|----|
| 1 | inventory_id | integer | NO | YES |
| 2 | film_id | smallint | NO | NO |
| 3 | store_id | smallint | NO | NO |
| 4 | last_update | timestamp without time zone | NO | NO |

### customer

| # | Column | Type | Nullable | PK |
|---|--------|------|----------|----|
| 1 | customer_id | integer | NO | YES |
| 2 | store_id | smallint | NO | NO |
| 3 | first_name | character varying | NO | NO |
| 4 | last_name | character varying | NO | NO |
| 5 | email | character varying | YES | NO |
| 6 | address_id | smallint | NO | NO |
| 7 | activebool | boolean | NO | NO |
| 8 | create_date | date | NO | NO |
| 9 | last_update | timestamp without time zone | YES | NO |
| 10 | active | integer | YES | NO |

### address

| # | Column | Type | Nullable | PK |
|---|--------|------|----------|----|
| 1 | address_id | integer | NO | YES |
| 2 | address | character varying | NO | NO |
| 3 | address2 | character varying | YES | NO |
| 4 | district | character varying | NO | NO |
| 5 | city_id | smallint | NO | NO |
| 6 | postal_code | character varying | YES | NO |
| 7 | phone | character varying | NO | NO |
| 8 | last_update | timestamp without time zone | NO | NO |

### film_category

| # | Column | Type | Nullable | PK |
|---|--------|------|----------|----|
| 1 | film_id | smallint | NO | YES |
| 2 | category_id | smallint | NO | YES |
| 3 | last_update | timestamp without time zone | NO | NO |

### city

| # | Column | Type | Nullable | PK |
|---|--------|------|----------|----|
| 1 | city_id | integer | NO | YES |
| 2 | city | character varying | NO | NO |
| 3 | country_id | smallint | NO | NO |
| 4 | last_update | timestamp without time zone | NO | NO |

### actor

| # | Column | Type | Nullable | PK |
|---|--------|------|----------|----|
| 1 | actor_id | integer | NO | YES |
| 2 | first_name | character varying | NO | NO |
| 3 | last_name | character varying | NO | NO |
| 4 | last_update | timestamp without time zone | NO | NO |

### store

| # | Column | Type | Nullable | PK |
|---|--------|------|----------|----|
| 1 | store_id | integer | NO | YES |
| 2 | manager_staff_id | smallint | NO | NO |
| 3 | address_id | smallint | NO | NO |
| 4 | last_update | timestamp without time zone | NO | NO |

### staff

| # | Column | Type | Nullable | PK |
|---|--------|------|----------|----|
| 1 | staff_id | integer | NO | YES |
| 2 | first_name | character varying | NO | NO |
| 3 | last_name | character varying | NO | NO |
| 4 | address_id | smallint | NO | NO |
| 5 | email | character varying | YES | NO |
| 6 | store_id | smallint | NO | NO |
| 7 | active | boolean | NO | NO |
| 8 | username | character varying | NO | NO |
| 9 | password | character varying | YES | NO |
| 10 | last_update | timestamp without time zone | NO | NO |
| 11 | picture | bytea | YES | NO |

### category

| # | Column | Type | Nullable | PK |
|---|--------|------|----------|----|
| 1 | category_id | integer | NO | YES |
| 2 | name | character varying | NO | NO |
| 3 | last_update | timestamp without time zone | NO | NO |

### country

| # | Column | Type | Nullable | PK |
|---|--------|------|----------|----|
| 1 | country_id | integer | NO | YES |
| 2 | country | character varying | NO | NO |
| 3 | last_update | timestamp without time zone | NO | NO |

### language

| # | Column | Type | Nullable | PK |
|---|--------|------|----------|----|
| 1 | language_id | integer | NO | YES |
| 2 | name | character | NO | NO |
| 3 | last_update | timestamp without time zone | NO | NO |

## Foreign Keys

| From Table | From Column | → | To Table | To Column |
|------------|-------------|---|----------|-----------|
