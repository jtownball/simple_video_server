import os
import psycopg2
from flask import Flask, render_template_string, send_file, jsonify

# Configuration
RENAMED_FOLDER = "renamed_videos"

# Database configuration
DB_CONFIG = {
    'dbname': 'video_library',
    'user': 'postgres',
    'password': 'your_password',  # Change this
    'host': 'localhost',
    'port': '5432'
}

app = Flask(__name__)

def get_db_connection():
    """Create and return a database connection"""
    return psycopg2.connect(**DB_CONFIG)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Video Player</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        h1 {
            color: #333;
        }
        .video-list {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }
        .video-item {
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            cursor: pointer;
            transition: transform 0.2s;
        }
        .video-item:hover {
            transform: translateY(-5px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.2);
        }
        .video-name {
            font-weight: bold;
            margin-bottom: 5px;
            color: #2c3e50;
        }
        .video-hash {
            font-size: 12px;
            color: #7f8c8d;
            font-family: monospace;
        }
        #video-player {
            margin-top: 30px;
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        video {
            width: 100%;
            max-width: 800px;
            border-radius: 4px;
        }
        #current-video {
            margin-top: 10px;
            font-style: italic;
            color: #34495e;
        }
    </style>
</head>
<body>
    <h1>Video Library</h1>
    <div class="video-list" id="videoList"></div>
    <div id="video-player" style="display:none;">
        <h2>Now Playing</h2>
        <video id="player" controls></video>
        <p id="current-video"></p>
    </div>

    <script>
        // Load video list
        fetch('/api/videos')
            .then(response => response.json())
            .then(data => {
                const list = document.getElementById('videoList');
                data.forEach(video => {
                    const item = document.createElement('div');
                    item.className = 'video-item';
                    item.innerHTML = `
                        <div class="video-name">${video.original_filename}</div>
                        <div class="video-hash">${video.hash_name}</div>
                    `;
                    item.onclick = () => playVideo(video.hash_name, video.original_filename);
                    list.appendChild(item);
                });
            });

        function playVideo(hash, name) {
            const player = document.getElementById('player');
            const playerDiv = document.getElementById('video-player');
            const currentVideo = document.getElementById('current-video');
            
            player.src = `/video/${hash}`;
            currentVideo.textContent = `Playing: ${name}`;
            playerDiv.style.display = 'block';
            player.play();
            
            // Scroll to player
            playerDiv.scrollIntoView({ behavior: 'smooth' });
        }
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/videos')
def get_videos():
    """Return all videos from the database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT hash_name, original_filename, file_extension, created_at
            FROM video_mapping
            ORDER BY original_filename
        """)
        
        videos = []
        for row in cursor.fetchall():
            videos.append({
                'hash_name': row[0],
                'original_filename': row[1],
                'file_extension': row[2],
                'created_at': row[3].isoformat() if row[3] else None
            })
        
        cursor.close()
        conn.close()
        
        return jsonify(videos)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/video/<hash_name>')
def serve_video(hash_name):
    """Serve a video file by its hash"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT file_extension FROM video_mapping WHERE hash_name = %s
        """, (hash_name,))
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result:
            ext = result[0]
            filename = f"{hash_name}{ext}"
            filepath = os.path.join(RENAMED_FOLDER, filename)
            
            if os.path.exists(filepath):
                return send_file(filepath)
        
        return "Video not found", 404
    except Exception as e:
        return f"Error: {str(e)}", 500

if __name__ == "__main__":
    print("Starting video player web server...")
    print("Open http://localhost:5000 in your browser")
    app.run(debug=True, port=5000)
