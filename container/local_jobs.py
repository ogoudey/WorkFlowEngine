import boto3
import uvicorn
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# create s3 client

def get_modality(robot_type: str):
    # perhaps we have a local storage of modality.json
    # s3_client.get the modality file
    pass

def composite_videos(video: str, overlay: str):
    import subprocess
    import sys

    if len(sys.argv) < 3:
        print("Usage: python3 composite.py <raw_video> <robot_overlay>")
        sys.exit(1)
    raw_video = sys.argv[1]
    overlay_video = sys.argv[2]
    output_path = "composite.mp4"
    try:
        subprocess.run([
            "ffmpeg",
            "-i", str(raw_video),       # background (raw video)
            "-i", str(overlay_video),   # foreground (robot on red background)
            "-filter_complex",
            "[1:v]colorkey=0xFF0000:0.3:0.1[fg];[0:v][fg]overlay",
            "-c:v", "libx264",
            str(output_path),
        ], check=True)
    except Exception as e:
        logger.error(f"Could not composite videos: {e}")
        return "FAILED"
    
    return "SUCCEEDED"