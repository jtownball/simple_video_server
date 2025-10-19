import os
import hashlib
import shutil
import psycopg2
from pathlib import Path
from datetime import timedelta

# Configuration
VIDEO_FOLDER = "videos"  # Change this to your folder path
RENAMED_FOLDER = "renamed_videos"

# Database configuration
DB_CONFIG = {
    'dbname': 'video_library',
    'user': 'postgres',
    'password': 'your_password',  # Change this
    'host': 'localhost',
    'port': '5432'
}

def generate_hash(filename, filepath):
    """Generate a unique hash for a filename"""
    unique_string = filename + str(os.path.getmtime(filepath)) + str(os.path.getsize(filepath))
    return hashlib.sha256(unique_string.encode()).hexdigest()[:16]

def get_db_connection():
    """Create and return a database connection"""
    return psycopg2.connect(**DB_CONFIG)

def get_video_metadata(filepath):
    """Extract video metadata using ffprobe if available"""
    metadata = {
        'duration': None,
        'width': None,
        'height': None,
        'codec': None,
        'bitrate': None,
        'file_size': os.path.getsize(filepath)
    }
    
    try:
        import subprocess
        import json
        
        # Use ffprobe to get video metadata
        result = subprocess.run([
            'ffprobe', '-v', 'quiet', '-print_format', 'json',
            '-show_format', '-show_streams', filepath
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            data = json.loads(result.stdout)
            
            # Get video stream info
            video_stream = next((s for s in data.get('streams', []) if s.get('codec_type') == 'video'), None)
            if video_stream:
                metadata['width'] = video_stream.get('width')
                metadata['height'] = video_stream.get('height')
                metadata['codec'] = video_stream.get('codec_name')
            
            # Get format info
            format_info = data.get('format', {})
            duration = format_info.get('duration')
            if duration:
                metadata['duration'] = int(float(duration))
            
            bitrate = format_info.get('bit_rate')
            if bitrate:
                metadata['bitrate'] = int(bitrate)
    
    except (ImportError, FileNotFoundError, subprocess.TimeoutExpired, Exception) as e:
        print(f"  Note: Could not extract metadata (ffprobe not available or error: {e})")
    
    return metadata

def file_exists_in_db(conn, original_filename):
    """Check if a file already exists in the database by original filename"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT hash_name FROM video_mapping WHERE original_filename = %s
    """, (original_filename,))
    result = cursor.fetchone()
    cursor.close()
    return result[0] if result else None

def add_new_videos():
    """Process video folder and add only NEW files to database"""
    if not os.path.exists(VIDEO_FOLDER):
        print(f"Error: Folder '{VIDEO_FOLDER}' does not exist")
        return
    
    # Create renamed folder if it doesn't exist
    os.makedirs(RENAMED_FOLDER, exist_ok=True)
    
    # Connect to database
    conn = get_db_connection()
    
    added_count = 0
    skipped_count = 0
    
    print("Scanning video folder for new files...\n")
    
    for filename in os.listdir(VIDEO_FOLDER):
        filepath = os.path.join(VIDEO_FOLDER, filename)
        
        # Skip directories
        if os.path.isdir(filepath):
            continue
        
        # Check if file already exists in database
        existing_hash = file_exists_in_db(conn, filename)
        if existing_hash:
            print(f"⏭️  SKIP: {filename} (already in database as {existing_hash})")
            skipped_count += 1
            continue
        
        # Get file extension
        _, ext = os.path.splitext(filename)
        
        # Generate unique hash
        hash_name = generate_hash(filename, filepath)
        new_filename = f"{hash_name}{ext}"
        new_filepath = os.path.join(RENAMED_FOLDER, new_filename)
        
        print(f"➕ ADD: {filename}")
        print(f"   Hash: {hash_name}")
        
        # Copy file with new name
        shutil.copy2(filepath, new_filepath)
        print(f"   ✓ File copied to {new_filename}")
        
        # Get video metadata
        print(f"   📊 Extracting metadata...")
        metadata = get_video_metadata(new_filepath)
        
        # Insert into database
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO video_mapping (
                    hash_name, original_filename, file_extension,
                    duration, width, height, codec, bitrate, file_size
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                hash_name, filename, ext,
                metadata['duration'], metadata['width'], metadata['height'],
                metadata['codec'], metadata['bitrate'], metadata['file_size']
            ))
            conn.commit()
            cursor.close()
            
            # Print metadata if available
            if metadata['duration']:
                duration_str = str(timedelta(seconds=metadata['duration']))
                print(f"   Duration: {duration_str}")
            if metadata['width'] and metadata['height']:
                print(f"   Resolution: {metadata['width']}x{metadata['height']}")
            if metadata['codec']:
                print(f"   Codec: {metadata['codec']}")
            
            print(f"   ✓ Added to database\n")
            added_count += 1
            
        except Exception as e:
            print(f"   ✗ Error adding to database: {e}\n")
            conn.rollback()
    
    conn.close()
    
    print("=" * 50)
    print(f"✓ Added: {added_count} new file(s)")
    print(f"⏭️  Skipped: {skipped_count} existing file(s)")
    print("=" * 50)

if __name__ == "__main__":
    try:
        add_new_videos()
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure the database is initialized (run init_database.py first)")
