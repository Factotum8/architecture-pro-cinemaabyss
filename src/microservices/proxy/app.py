import os
import random
import logging
from urllib.parse import urljoin

from fastapi import FastAPI, Request, HTTPException
import requests

app = FastAPI(title="CinemaAbyss API Gateway")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Backend service URLs (could be in config or env variables)
SERVICES = {
    "videos": "http://movies-service:8081",
    "monolith": " http://monolith:8080",
    "event": "http://events-service:8082",
}

# Generic request forwarder
async def forward_request(service: str, path: str, request: Request):
    if service not in SERVICES:
        raise HTTPException(status_code=502, detail="Unknown service")

    url = urljoin(SERVICES[service], path)

    # monkey patch
    if url[-1] == "/":
        url = url[:-1]

    logger.debug(f"Forwarding request to {url}")

    try:
        resp= requests.request(method=request.method, url=url)
        logger.debug(f"Forwarding response: {resp}")
        return resp
    except Exception as e:
            raise HTTPException(status_code=502, detail=f"Service unavailable: {e}")

# Routes
@app.get("/health")
async def root():
    return {"message": "Welcome to CinemaAbyss API Gateway"}

@app.api_route("/api/movies", methods=["GET", "POST", "PUT", "DELETE"])
async def videos_proxy(request: Request):
    logger.debug(f"user_proxy request: {request}")

    if random.randint(1, 100) <= int(os.getenv("MOVIES_MIGRATION_PERCENT")):
        resp = await forward_request("videos", f"/api/movies/", request)
    else:
        resp = await forward_request("monolith/", f"/api/movies/", request)

    r = resp.json()
    logger.debug(f"Movies proxy response: {r}")
    return r

@app.api_route("/api/users", methods=["GET", "POST", "PUT", "DELETE"])
async def user_proxy(request: Request):
    logger.debug(f"user_proxy request: {request}")
    resp = await forward_request("monolith", f"/api/users/", request)
    return resp.json()

@app.api_route("/api/event", methods=["GET", "POST", "PUT", "DELETE"])
async def discounts_proxy(request: Request):
    resp = await forward_request("event", f"/api/event/", request)
    return resp.json()
