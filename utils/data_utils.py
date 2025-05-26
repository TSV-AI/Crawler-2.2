# utils/data_utils.py
import csv
import asyncpg # For PostgreSQL interaction
import os
import asyncio

# Assuming your Venue model is in models/venue.py
# This is used by save_venues_to_csv and for reference in create_venues_table.
from models.venue import Venue


# --- Existing Utility Functions ---
def is_duplicate_venue(venue_name: str, seen_names: set) -> bool:
    return venue_name in seen_names


def is_complete_venue(venue: dict, required_keys: list) -> bool:
    return all(key in venue for key in required_keys)


def save_venues_to_csv(venues: list, filename: str):
    if not venues:
        print("No venues to save to CSV.")
        return

    # Use field names from the Venue model
    # For Pydantic v2, it's model_fields. For v1, it might be __fields__.
    # Ensure your Venue model is Pydantic v2 if using model_fields.
    fieldnames = list(Venue.model_fields.keys()) if hasattr(Venue, 'model_fields') else list(Venue.__fields__.keys())


    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        # Ensure all dictionaries in 'venues' have all keys in 'fieldnames'
        # or handle missing keys appropriately (e.g., by providing default values)
        processed_venues = []
        for venue in venues:
            processed_venue = {key: venue.get(key) for key in fieldnames}
            processed_venues.append(processed_venue)
        writer.writerows(processed_venues)
    print(f"Saved {len(venues)} venues to '{filename}'.")


# --- New Database Utility Functions ---

DB_POOL = None # Global variable to hold the connection pool

async def init_db_pool():
    """
    Initializes the asyncpg connection pool using environment variables.
    Prioritizes DATABASE_URL, then falls back to individual PG* variables.
    Also creates the 'venues' table if it doesn't exist.
    """
    global DB_POOL
    if DB_POOL is not None:
        print("DEBUG: Database pool already initialized.")
        return

    try:
        database_url = os.getenv('DATABASE_URL') # Railway typically provides this
        if not database_url:
            print("DEBUG: DATABASE_URL not found, constructing from individual PG* environment variables.")
            pg_user = os.getenv('PGUSER', os.getenv('POSTGRES_USER'))
            pg_password = os.getenv('PGPASSWORD', os.getenv('POSTGRES_PASSWORD'))
            pg_db = os.getenv('PGDATABASE', os.getenv('POSTGRES_DB'))
            pg_host = os.getenv('PGHOST') # e.g., RAILWAY_PRIVATE_DOMAIN or specific DB host
            pg_port = os.getenv('PGPORT', '5432')

            if not all([pg_user, pg_password, pg_db, pg_host]):
                print("DEBUG: CRITICAL ERROR - Missing one or more core database connection environment variables (PGUSER, PGPASSWORD, PGDATABASE, PGHOST).")
                raise ValueError("Missing core database connection environment variables.")
            
            database_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"

        # Hide password from logs if present in the URL string
        log_url = database_url
        temp_password_for_log = os.getenv('PGPASSWORD', os.getenv('POSTGRES_PASSWORD'))
        if temp_password_for_log:
             log_url = log_url.replace(temp_password_for_log, "********")
        
        print(f"DEBUG: Attempting to connect to database using DSN derived from: {log_url}")
        
        DB_POOL = await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=10)
        print("DEBUG: Database connection pool created successfully.")
        
        # Create table after pool is initialized
        await create_venues_table()

    except Exception as e:
        print(f"DEBUG: CRITICAL ERROR - Could not create database connection pool: {e}")
        DB_POOL = None # Ensure pool is None if initialization failed
        raise 

async def create_venues_table():
    """
    Creates the 'venues' table in the database if it doesn't already exist.
    The schema is based on the Venue Pydantic model.
    """
    if not DB_POOL:
        print("DEBUG: Database pool not initialized. Cannot create 'venues' table.")
        raise ConnectionError("Database pool not initialized.")

    async with DB_POOL.acquire() as connection:
        try:
            await connection.execute("""
                CREATE TABLE IF NOT EXISTS venues (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    location TEXT,
                    price TEXT,
                    capacity TEXT,
                    rating REAL,
                    reviews INTEGER,
                    description TEXT,
                    scraped_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT unique_venue_name_location UNIQUE (name, location)
                );
            """)
            print("DEBUG: 'venues' table checked/created successfully.")
        except Exception as e:
            print(f"DEBUG: Error creating 'venues' table: {e}")
            raise

async def save_venues_to_db(venues_list: list):
    """
    Saves a list of venue dictionaries/objects to the 'venues' table in PostgreSQL.
    Uses ON CONFLICT DO NOTHING to avoid duplicates based on (name, location) constraint.
    """
    if not DB_POOL:
        print("DEBUG: Database pool not initialized. Cannot save venues to DB.")
        raise ConnectionError("Database pool not initialized.")
        
    if not venues_list:
        print("No venues to save to database.")
        return 0

    async with DB_POOL.acquire() as connection:
        data_to_insert = []
        for venue in venues_list:
            # Ensure venue is a dict, if it's a Pydantic model, convert it
            venue_dict = venue if isinstance(venue, dict) else venue.model_dump() # Pydantic v2

            data_to_insert.append((
                venue_dict.get('name'),
                venue_dict.get('location'),
                venue_dict.get('price'),
                venue_dict.get('capacity'),
                float(venue_dict.get('rating', 0.0) if venue_dict.get('rating') is not None else 0.0),
                int(venue_dict.get('reviews', 0) if venue_dict.get('reviews') is not None else 0),
                venue_dict.get('description')
            ))
        
        try:
            result = await connection.executemany(
                """
                INSERT INTO venues (name, location, price, capacity, rating, reviews, description)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (name, location) DO NOTHING;
                """,
                data_to_insert
            )
            print(f"Data for {len(venues_list)} venues processed for database insertion/conflict check.")
            return len(venues_list)

        except Exception as e:
            print(f"DEBUG: Error during batch insert into 'venues' table: {e}")
            import traceback
            traceback.print_exc()
            return 0

async def close_db_pool():
    """
    Closes the asyncpg connection pool.
    """
    global DB_POOL
    if DB_POOL:
        try:
            await DB_POOL.close()
            print("DEBUG: Database connection pool closed.")
        except Exception as e:
            print(f"DEBUG: Error closing database pool: {e}")
        finally:
            DB_POOL = None
