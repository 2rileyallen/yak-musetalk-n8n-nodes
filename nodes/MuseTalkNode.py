import asyncio
import json
import uuid
import os
import base64
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect
from gradio_client import Client, handle_file
import uvicorn

# --- Configuration ---
MUSE_TALK_ADDRESS = "http://127.0.0.1:7860/"
GATEKEEPER_PORT = 7861 # Use a different port from the ComfyUI gatekeeper

# --- State and Concurrency Control ---
# This lock ensures that only one Gradio inference runs at a time.
processing_lock = asyncio.Lock()

# --- WebSocket Connection Manager ---
class ConnectionManager:
    """Manages active WebSocket connections."""
    def __init__(self):
        self.active_connections: dict[str, WebSocket] = {}

    async def connect(self, job_id: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[job_id] = websocket
        print(f"[WS-CONN] WebSocket connected for job_id: {job_id}")

    def disconnect(self, job_id: str):
        if job_id in self.active_connections:
            del self.active_connections[job_id]
            print(f"[WS-DCONN] WebSocket disconnected for job_id: {job_id}")

    async def send_result(self, job_id: str, data: dict):
        if job_id in self.active_connections:
            websocket = self.active_connections[job_id]
            try:
                print(f"[WS-SEND] Sending result to job {job_id}")
                await websocket.send_json(data)
            except Exception as e:
                print(f"[ERROR] Failed to send WebSocket message for job {job_id}: {e}")

manager = ConnectionManager()

# --- Main Processing Logic ---
async def run_musetalk_inference(job_params: dict):
    """
    Connects to the Gradio client and runs the inference.
    This function is called under the processing_lock.
    """
    print(f"Starting inference for job...")
    try:
        client = Client(MUSE_TALK_ADDRESS)
        result = client.predict(
            audio_path=handle_file(job_params["audio_path"]),
            video_path={"video": handle_file(job_params["video_path"])},
            bbox_shift=job_params["bbox_shift"],
            extra_margin=job_params["extra_margin"],
            parsing_mode=job_params["parsing_mode"],
            left_cheek_width=job_params["left_cheek_width"],
            right_cheek_width=job_params["right_cheek_width"],
            api_name="/inference"
        )
        print(f"Inference successful. Result: {result}")

        # The result is a tuple, the first element contains the video info
        output_video_info = result[0]
        # The path is a temporary file path created by Gradio
        temp_output_path = output_video_info.get('video')

        if not temp_output_path:
             raise ValueError("Inference did not return a video file path.")

        # Move the temporary file to the desired final output path
        final_output_path = job_params["output_file_path"]
        os.rename(temp_output_path, final_output_path)
        print(f"Output video moved to: {final_output_path}")

        return final_output_path

    except Exception as e:
        print(f"[ERROR] MuseTalk inference failed: {e}")
        # Re-raise the exception to be caught by the endpoint handler
        raise

# --- Lifespan Event Handler ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"[INFO] MuseTalk Gatekeeper starting up on port {GATEKEEPER_PORT}.")
    yield
    print("[INFO] MuseTalk Gatekeeper server shutting down.")

# --- FastAPI Application ---
app = FastAPI(title="MuseTalk Gatekeeper", lifespan=lifespan)

# --- API Endpoints ---
@app.post("/execute")
async def execute_workflow(request: Request):
    """
    Accepts a job, adds it to the queue, and returns a job_id.
    The actual processing happens in the background.
    """
    job_params = await request.json()
    job_id = str(uuid.uuid4())
    print(f"[JOB-RECEIVED] Job {job_id} received. Params: {job_params}")

    # This function runs in the background, not awaited here.
    # This makes the endpoint return immediately.
    asyncio.create_task(process_job(job_id, job_params))

    return {"status": "success", "job_id": job_id}


async def process_job(job_id: str, job_params: dict):
    """
    The main background task that acquires the lock and runs the job.
    """
    final_result = {}
    try:
        # Wait until the lock is available
        async with processing_lock:
            print(f"[LOCK-ACQUIRED] Processing job {job_id}")
            output_path = await run_musetalk_inference(job_params)
            
            # Prepare success result
            final_result = {
                "format": "filePath",
                "data": output_path,
                "filename": os.path.basename(output_path)
            }

    except Exception as e:
        print(f"[JOB-ERROR] Job {job_id} failed with error: {e}")
        final_result = {
            "format": "error",
            "error": str(e)
        }
    finally:
        print(f"[LOCK-RELEASED] Finished processing job {job_id}")
        # Send the final result (success or error) via WebSocket
        await manager.send_result(job_id, final_result)


@app.websocket("/ws/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
    """Handles WebSocket connections for clients waiting for results."""
    await manager.connect(job_id, websocket)
    try:
        # Keep the connection alive until the server sends a message
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(job_id)

# --- Main Execution ---
if __name__ == "__main__":
    uvicorn.run("muse_talk_node:app", host="0.0.0.0", port=GATEKEEPER_PORT, reload=True)
