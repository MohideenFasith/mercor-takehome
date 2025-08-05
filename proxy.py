from fastapi import FastAPI
from pydantic import BaseModel
import httpx
import time
from collections import deque
from threading import Lock, Thread
import uuid

CLASSIFICATION_SERVER_URL = "http://localhost:8001/classify"

app = FastAPI()

class ProxyRequest(BaseModel):
    sequence: str

class ProxyResponse(BaseModel):
    result: str

# Separate queues for Client A (short) and Client B (long)
queue_a = deque()
queue_b = deque()
response_dict = {}
proxy_lock = Lock()

def classify_request_group(batch, id_list):
    try:
        print(f"Dispatching batch of {len(batch)} sequences to classification server.")
        response = httpx.post(CLASSIFICATION_SERVER_URL, json={"sequences": batch})
        response.raise_for_status()
        results = response.json()["results"]
        with proxy_lock:
            for req_id, result in zip(id_list, results):
                response_dict[req_id] = result
        print(f"Batch processed. Results: {results}")
    except Exception as e:
        print(f"Error processing batch: {e}")
        with proxy_lock:
            for req_id, seq in zip(id_list, batch):
                # Re-enqueue into appropriate queue
                (queue_a if len(seq) < 100 else queue_b).appendleft((req_id, seq))

def batch_dispatcher():
    toggle = True  # Toggle between queues
    while True:
        time.sleep(0.01)
        batch = []
        id_list = []

        with proxy_lock:
            for _ in range(5):
                target_queue = queue_a if toggle else queue_b
                if target_queue:
                    req_id, text = target_queue.popleft()
                    batch.append(text)
                    id_list.append(req_id)
                toggle = not toggle  # Alternate after every item

        if batch:
            classify_request_group(batch, id_list)

Thread(target=batch_dispatcher, daemon=True).start()

@app.post("/proxy_classify")
def proxy_classify(req: ProxyRequest) -> ProxyResponse:
    req_id = str(uuid.uuid4())
    queue_type = "A" if len(req.sequence) < 100 else "B"
    print(f"Received request from client {queue_type}. Length: {len(req.sequence)}")

    with proxy_lock:
        if queue_type == "A":
            queue_a.append((req_id, req.sequence))
        else:
            queue_b.append((req_id, req.sequence))

    while True:
        with proxy_lock:
            if req_id in response_dict:
                result = response_dict.pop(req_id)
                print(f"Returning result for {req_id}: {result}")
                return ProxyResponse(result=result)
        time.sleep(0.01)
