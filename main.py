"""Rotterdam API — Haven Platform test backend.

Tests ALL managed service types: PostgreSQL, Redis, RabbitMQ, MongoDB, MySQL.
Each service is auto-detected via environment variables injected by Haven.
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

logger = logging.getLogger("rotterdam-api")

# Config from environment (auto-injected by Haven when services are connected)
DATABASE_URL = os.getenv("DATABASE_URL", "")
REDIS_URL = os.getenv("REDIS_URL", "")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "")
# MongoDB URL: prefer DATABASE_URL if it starts with mongodb://, then MONGODB_URL
_db_url = os.getenv("DATABASE_URL", "")
MONGODB_URL = (_db_url if _db_url.startswith("mongodb://") else "") or os.getenv("MONGODB_URL", "") or os.getenv("MONGO_URL", "")
MYSQL_URL = os.getenv("MYSQL_URL", "")
# MySQL individual fields (fallback if MYSQL_URL not set)
MYSQL_HOST = os.getenv("DB_HOST", "")
MYSQL_PORT = int(os.getenv("DB_PORT", "3306"))
MYSQL_USER = os.getenv("DB_USER", "")
MYSQL_PASSWORD = os.getenv("DB_PASSWORD", "")
MYSQL_DB = os.getenv("DB_NAME", "mysql")
PORT = int(os.getenv("PORT", "8080"))

# Connection state
services = {"postgres": False, "redis": False, "rabbitmq": False, "mongodb": False, "mysql": False}


async def check_postgres():
    if not DATABASE_URL or not DATABASE_URL.startswith("postgresql"):
        return
    try:
        import asyncpg

        conn = await asyncpg.connect(DATABASE_URL, timeout=10)
        await conn.execute("SELECT 1")
        await conn.close()
        services["postgres"] = True
        logger.info("PostgreSQL connected: %s", DATABASE_URL.split("@")[-1])
    except Exception as e:
        logger.warning("PostgreSQL not available: %s", e)


async def check_redis():
    if not REDIS_URL:
        return
    try:
        import redis.asyncio as aioredis

        r = aioredis.from_url(REDIS_URL)
        await r.ping()
        await r.close()
        services["redis"] = True
        logger.info("Redis connected: %s", REDIS_URL)
    except Exception as e:
        logger.warning("Redis not available: %s", e)


async def check_rabbitmq():
    if not RABBITMQ_URL:
        return
    try:
        import aio_pika

        conn = await aio_pika.connect_robust(RABBITMQ_URL, timeout=10)
        await conn.close()
        services["rabbitmq"] = True
        logger.info("RabbitMQ connected: %s", RABBITMQ_URL.split("@")[-1])
    except Exception as e:
        logger.warning("RabbitMQ not available: %s", e)


async def check_mongodb():
    mongo_url = MONGODB_URL
    if not mongo_url:
        return
    try:
        from pymongo import MongoClient

        client = MongoClient(mongo_url, serverSelectionTimeoutMS=10000)
        db = client.get_default_database()
        if db is not None:
            db.command("ping")
        else:
            client.admin.command("ping")
        client.close()
        services["mongodb"] = True
        logger.info("MongoDB connected: %s", mongo_url.split("@")[-1] if "@" in mongo_url else mongo_url)
    except Exception as e:
        logger.warning("MongoDB not available: %s", e)


async def check_mysql():
    if not MYSQL_HOST and not MYSQL_URL:
        return
    try:
        import aiomysql

        if MYSQL_URL and MYSQL_URL.startswith("mysql://"):
            # Parse URL: mysql://user:pass@host:port/db
            from urllib.parse import urlparse

            parsed = urlparse(MYSQL_URL)
            host = parsed.hostname or "localhost"
            port = parsed.port or 3306
            user = parsed.username or "root"
            password = parsed.password or ""
            db = parsed.path.lstrip("/") or "mysql"
        else:
            host, port, user, password, db = MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB

        conn = await aiomysql.connect(host=host, port=port, user=user, password=password, db=db, connect_timeout=10)
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")
        conn.close()
        services["mysql"] = True
        logger.info("MySQL connected: %s:%d/%s", host, port, db)
    except Exception as e:
        logger.warning("MySQL not available: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO)
    logger.info("Rotterdam API starting on port %d", PORT)
    # Run checks in background — don't block startup (liveness probe needs fast start)
    asyncio.create_task(_run_checks())
    yield


async def _run_checks():
    """Run connectivity checks after startup (non-blocking)."""
    await asyncio.sleep(2)  # Let server start first
    await asyncio.gather(check_postgres(), check_redis(), check_rabbitmq(), check_mongodb(), check_mysql())


app = FastAPI(title="Rotterdam API", lifespan=lifespan)


def _status(name: str, env_var: str) -> str:
    if services[name]:
        return "connected"
    return "not configured" if not env_var else "disconnected"


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "rotterdam-api",
        "connections": {
            "postgres": _status("postgres", DATABASE_URL),
            "redis": _status("redis", REDIS_URL),
            "rabbitmq": _status("rabbitmq", RABBITMQ_URL),
            "mongodb": _status("mongodb", MONGODB_URL),
            "mysql": _status("mysql", MYSQL_URL or MYSQL_HOST),
        },
    }


@app.get("/")
async def root():
    return {"message": "Rotterdam API — Haven Platform test app", "port": PORT}


@app.get("/db-test")
async def db_test():
    """Test PostgreSQL connectivity — SELECT version()."""
    if not DATABASE_URL or not DATABASE_URL.startswith("postgresql"):
        return {"error": "DATABASE_URL not configured or not PostgreSQL"}
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
    """Test Redis connectivity — SET/GET."""
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


@app.get("/mongo-test")
async def mongo_test():
    """Test MongoDB connectivity — insert + find."""
    if not MONGODB_URL:
        return {"error": "MONGODB_URL not configured"}
    try:
        from pymongo import MongoClient

        client = MongoClient(MONGODB_URL, serverSelectionTimeoutMS=10000)
        db = client.get_default_database()
        if db is None:
            db = client["haven_test"]
        collection = db["connectivity_test"]
        collection.insert_one({"test": "haven", "status": "ok"})
        doc = collection.find_one({"test": "haven"})
        collection.delete_many({"test": "haven"})
        client.close()
        return {"mongodb_status": "ok", "document_found": doc is not None}
    except Exception as e:
        return {"error": str(e)}


@app.get("/mysql-test")
async def mysql_test():
    """Test MySQL connectivity — SELECT version()."""
    if not MYSQL_HOST and not MYSQL_URL:
        return {"error": "MYSQL_URL or DB_HOST not configured"}
    try:
        import aiomysql
        from urllib.parse import urlparse

        if MYSQL_URL and MYSQL_URL.startswith("mysql://"):
            parsed = urlparse(MYSQL_URL)
            host = parsed.hostname or "localhost"
            port = parsed.port or 3306
            user = parsed.username or "root"
            password = parsed.password or ""
            db = parsed.path.lstrip("/") or "mysql"
        else:
            host, port, user, password, db = MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB

        conn = await aiomysql.connect(host=host, port=port, user=user, password=password, db=db, connect_timeout=10)
        async with conn.cursor() as cur:
            await cur.execute("SELECT version()")
            result = await cur.fetchone()
        conn.close()
        return {"mysql_version": result[0] if result else None}
    except Exception as e:
        return {"error": str(e)}


@app.get("/rabbit-test")
async def rabbit_test():
    """Test RabbitMQ — send + receive message."""
    if not RABBITMQ_URL:
        return {"error": "RABBITMQ_URL not configured"}
    try:
        import aio_pika

        conn = await aio_pika.connect_robust(RABBITMQ_URL, timeout=10)
        channel = await conn.channel()
        queue = await channel.declare_queue("haven_test", auto_delete=True)
        await channel.default_exchange.publish(
            aio_pika.Message(body=b"haven-connectivity-test"),
            routing_key="haven_test",
        )
        msg = await queue.get(timeout=5)
        await msg.ack()
        await conn.close()
        return {"rabbitmq_status": "ok", "message_received": msg.body.decode()}
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
