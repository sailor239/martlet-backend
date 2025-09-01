# Martlet Backend


## Database Migration:
1. Generate a new migration:  
    ```
    dbmate new create_users_table
    ```  
    This creates a timestamped migration file like db/migrations/20231201120000_create_users_table.sql

2. Write your migration:  
    ```
    -- migrate:up
    CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- migrate:down
    DROP TABLE users;
    ```

3. Running Migrations
   - Apply pending migrations:
        ```
        dbmate up
        ```
    - Rollback migrations:
        ```
        dbmate down    # rollback last migration
        dbmate down 2  # rollback last 2 migrations
        ```

### Tips:
- Use dbmate status to check which migrations are pending
