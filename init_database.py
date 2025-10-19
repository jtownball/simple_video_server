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
        print(f"✓ Database '{DB_CONFIG['dbname']}' created successfully")
    else:
        print(f"✓ Database '{DB_CONFIG['dbname']}' already exists")
    
    cursor.close()
    conn.close()

def create_tables():
    """Create the video_mapping table with metadata fields"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Create table with metadata fields
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS video_mapping (
            id SERIAL PRIMARY KEY,
            hash_name VARCHAR(255) UNIQUE NOT NULL,
            original_filename VARCHAR(500) UNIQUE NOT NULL,
            file_extension VARCHAR(10) NOT NULL,
            duration INTEGER,
            width INTEGER,
            height INTEGER,
            codec VARCHAR(50),
            bitrate BIGINT,
            file_size BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create index on hash_name for faster lookups
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_hash_name 
        ON video_mapping(hash_name)
    """)
    
    # Create index on original_filename for duplicate checking
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_original_filename 
        ON video_mapping(original_filename)
    """)
    
    conn.commit()
    print("✓ Table 'video_mapping' created successfully")
    print("✓ Indexes created successfully")
    print("\nTable schema:")
    print("  - hash_name: Unique hash identifier")
    print("  - original_filename: Original file name")
    print("  - file_extension: File extension (.mp4, .avi, etc)")
    print("  - duration: Video duration in seconds")
    print("  - width: Video width in pixels")
    print("  - height: Video height in pixels")
    print("  - codec: Video codec (h264, etc)")
    print("  - bitrate: Video bitrate")
    print("  - file_size: File size in bytes")
    print("  - created_at: Timestamp when added")
    
    cursor.close()
    conn.close()

def main():
    try:
        print("Initializing video library database...\n")
        create_database()
        create_tables()
        print("\n" + "=" * 50)
        print("Database initialization complete!")
        print("=" * 50)
        print(f"Database: {DB_CONFIG['dbname']}")
        print(f"Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        print("\nNext step: Run add_videos.py to add videos to the library")
    except Exception as e:
        print(f"✗ Error: {e}")
        print("\nMake sure PostgreSQL is running and credentials are correct.")

if __name__ == "__main__":
    main()
