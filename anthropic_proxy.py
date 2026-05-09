from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
import httpx
import uuid
import time
import json

app = FastAPI()

# ==============================
# CONFIG
# ==============================
OLLAMA_URL = "http://127.0.0.1:11434/api/chat"
MODEL_NAME = "qwen2.5-coder:32b"  # пример

# ==============================
# UTILS
# ==============================
def now():
    return int(time.time())

def anthropic_message_from_ollama(text: str):
    return {
        "id": f"msg_{uuid.uuid4().hex}",
        "type": "message",
        "role": "assistant",
        "content": [
            {"type": "text", "text": text}
        ]
    }

# ==============================
# /v1/models
# ==============================
@app.get("/v1/models")
async def list_models():
    return {
        "data": [
            {
                "id": "claude-sonnet-4.5",
                "type": "model",
                "created_at": now(),
                "display_name": "Claude Sonnet 4.5 (Proxy)"
            }
        ]
    }

# ==============================
# /v1/me
# ==============================
@app.get("/v1/me")
async def me():
    return {
        "id": "user_proxy",
        "type": "user",
        "email": "local@proxy"
    }

# ==============================
# /v1/messages (MAIN)
# ==============================
@app.post("/v1/messages")
async def messages(req: Request):
    body = await req.json()

    messages = body.get("messages", [])
    system = body.get("system", "")
    max_tokens = body.get("max_tokens", 4096)

    prompt = ""
    if system:
        prompt += f"SYSTEM:\n{system}\n\n"

    for m in messages:
        role = m["role"]
        for c in m["content"]:
            if c["type"] == "text":
                prompt += f"{role.upper()}:\n{c['text']}\n\n"

    async with httpx.AsyncClient(timeout=300) as client:
        r = await client.post(
            OLLAMA_URL,
            json={
                "model": MODEL_NAME,
                "messages": [
                    {"role": "user", "content": prompt}
                ],
                "stream": False
            }
        )

    data = r.json()
    text = data["message"]["content"]

    response = {
        "id": f"msg_{uuid.uuid4().hex}",
        "type": "message",
        "role": "assistant",
        "model": "claude-sonnet-4.5",
        "content": [
            {"type": "text", "text": text}
        ],
        "stop_reason": "end_turn"
    }

    return JSONResponse(response)

# ==============================
# /v1/complete (fallback)
# ==============================
@app.post("/v1/complete")
async def complete(req: Request):
    body = await req.json()
    prompt = body.get("prompt", "")

    async with httpx.AsyncClient(timeout=300) as client:
        r = await client.post(
            OLLAMA_URL,
            json={
                "model": MODEL_NAME,
                "messages": [
                    {"role": "user", "content": prompt}
                ],
                "stream": False
            }
        )

    text = r.json()["message"]["content"]

    return {
        "completion": text,
        "stop_reason": "stop_sequence"
    }

