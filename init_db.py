import psycopg2
from psycopg2 import sql

# Database configuration
DB_CONFIG = {
    'dbname': 'video_library',
    'user': 'postgres',
    'password': 'your_password',  # Change this
    'host': 'localhost',
    'port': '5432'
}

def create_database():
    """Create the database if it doesn't exist"""
    # Connect to PostgreSQL server (to the default 'postgres' database)
    conn = psycopg2.connect(
        dbname='postgres',
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port']
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Check if database exists
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_CONFIG['dbname'],))
    exists = cursor.fetchone()
    
    if not exists:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(
            sql.Identifier(DB_CONFIG['dbname'])
        ))
        print(f"Database '{DB_CONFIG['dbname']}' created successfully")
    else:
        print(f"Database '{DB_CONFIG['dbname']}' already exists")
    
    cursor.close()
    conn.close()

def create_tables():
    """Create the video_mapping table"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS video_mapping (
            id SERIAL PRIMARY KEY,
            hash_name VARCHAR(255) UNIQUE NOT NULL,
            original_filename VARCHAR(500) NOT NULL,
            file_extension VARCHAR(10) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create index on hash_name for faster lookups
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_hash_name 
        ON video_mapping(hash_name)
    """)
    
    conn.commit()
    print("Table 'video_mapping' created successfully")
    print("Index on 'hash_name' created successfully")
    
    cursor.close()
    conn.close()

def main():
    try:
        print("Initializing video library database...")
        create_database()
        create_tables()
        print("\nDatabase initialization complete!")
        print(f"Database: {DB_CONFIG['dbname']}")
        print(f"Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    except Exception as e:
        print(f"Error: {e}")
        print("\nMake sure PostgreSQL is running and credentials are correct.")

if __name__ == "__main__":
    main()
