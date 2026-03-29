"""Rotterdam API — Haven Platform test backend.

Uses PostgreSQL, Redis, and RabbitMQ to validate managed service connectivity.
"""

import os
import asyncio
import logging

from fastapi import FastAPI
from contextlib import asynccontextmanager

logger = logging.getLogger("rotterdam-api")

# Config from environment (auto-injected by Haven when services are connected)
DATABASE_URL = os.getenv("DATABASE_URL", "")
REDIS_URL = os.getenv("REDIS_URL", "")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "")
PORT = int(os.getenv("PORT", "8080"))

# Connection state
db_ok = False
redis_ok = False
rabbitmq_ok = False


async def check_postgres():
    global db_ok
    if not DATABASE_URL:
        return
    try:
        import asyncpg

        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute("SELECT 1")
        await conn.close()
        db_ok = True
        logger.info("PostgreSQL connected: %s", DATABASE_URL.split("@")[-1])
    except Exception as e:
        logger.warning("PostgreSQL not available: %s", e)


async def check_redis():
    global redis_ok
    if not REDIS_URL:
        return
    try:
        import redis.asyncio as aioredis

        r = aioredis.from_url(REDIS_URL)
        await r.ping()
        await r.close()
        redis_ok = True
        logger.info("Redis connected: %s", REDIS_URL)
    except Exception as e:
        logger.warning("Redis not available: %s", e)


async def check_rabbitmq():
    global rabbitmq_ok
    if not RABBITMQ_URL:
        return
    try:
        import aio_pika

        conn = await aio_pika.connect_robust(RABBITMQ_URL)
        await conn.close()
        rabbitmq_ok = True
        logger.info("RabbitMQ connected: %s", RABBITMQ_URL.split("@")[-1])
    except Exception as e:
        logger.warning("RabbitMQ not available: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO)
    logger.info("Rotterdam API starting on port %d", PORT)
    await asyncio.gather(check_postgres(), check_redis(), check_rabbitmq())
    yield


app = FastAPI(title="Rotterdam API", lifespan=lifespan)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "rotterdam-api",
        "connections": {
            "postgres": "connected" if db_ok else ("not configured" if not DATABASE_URL else "disconnected"),
            "redis": "connected" if redis_ok else ("not configured" if not REDIS_URL else "disconnected"),
            "rabbitmq": "connected" if rabbitmq_ok else ("not configured" if not RABBITMQ_URL else "disconnected"),
        },
    }


@app.get("/")
async def root():
    return {"message": "Rotterdam API — Haven Platform test app", "port": PORT}


@app.get("/db-test")
async def db_test():
    if not DATABASE_URL:
        return {"error": "DATABASE_URL not configured"}
    try:
        import asyncpg

        conn = await asyncpg.connect(DATABASE_URL)
        result = await conn.fetchval("SELECT version()")
        await conn.close()
        return {"postgres_version": result}
    except Exception as e:
        return {"error": str(e)}


@app.get("/redis-test")
async def redis_test():
    if not REDIS_URL:
        return {"error": "REDIS_URL not configured"}
    try:
        import redis.asyncio as aioredis

        r = aioredis.from_url(REDIS_URL)
        await r.set("haven_test", "rotterdam")
        val = await r.get("haven_test")
        await r.close()
        return {"redis_value": val.decode() if val else None}
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
