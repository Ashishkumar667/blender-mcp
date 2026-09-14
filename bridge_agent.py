#!/usr/bin/env python3
"""Local bridge agent for BlenderMCP's hosted, multi-user mode.

When BlenderMCP's server is deployed once and shared by many users (for
example hosted on a platform like on-demand.io), that server has no way to
reach into any individual user's machine -- each user's Blender only exists
on their own computer, which isn't reachable from the outside by default.

This script solves that by connecting outward instead of waiting to be
called into: run it on your own machine, next to Blender, and it opens an
outbound connection to the hosted server, then proxies commands between that
server and your local Blender addon socket. No inbound port or tunnel is
needed on your side.

Usage:
    pip install websockets
    python bridge_agent.py --relay-url wss://<your-hosted-server>/agent --key YOUR_KEY

`YOUR_KEY` is a value you choose yourself (any unique string works, e.g. a
UUID) -- use that same value as the blender_key query parameter in the MCP
server URL configured on the hosted platform, e.g.:
    https://<your-hosted-server>/mcp?blender_key=YOUR_KEY

Flags can also be set via environment variables:
    BLENDER_BRIDGE_RELAY_URL, BLENDER_BRIDGE_KEY, BLENDER_HOST, BLENDER_PORT
"""
import argparse
import asyncio
import json
import logging
import os
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("BlenderBridgeAgent")

RECONNECT_DELAY = 5.0
BLENDER_RECV_TIMEOUT = 180.0  # Matches the addon's own command timeout


def _with_key(relay_url: str, key: str) -> str:
    """Adds ?key=<key> to the relay URL (preserving any existing query)."""
    parsed = urlparse(relay_url)
    query = dict(parse_qsl(parsed.query))
    query["key"] = key
    return urlunparse(parsed._replace(query=urlencode(query)))


async def _recv_full_json(reader: asyncio.StreamReader) -> bytes:
    """Reads from the Blender addon socket until a complete JSON object has
    arrived, matching the framing the addon and MCP server already use."""
    chunks = b""
    while True:
        chunk = await asyncio.wait_for(reader.read(8192), timeout=BLENDER_RECV_TIMEOUT)
        if not chunk:
            if not chunks:
                raise ConnectionError("Blender closed the connection")
            break
        chunks += chunk
        try:
            json.loads(chunks.decode("utf-8"))
            return chunks
        except json.JSONDecodeError:
            continue
    return chunks


async def _run_session(relay_url: str, key: str, blender_host: str, blender_port: int):
    url = _with_key(relay_url, key)
    logger.info(f"Connecting to relay at {url}")

    async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
        logger.info("Connected to hosted server. Waiting for commands...")
        blender_writer = None
        blender_reader = None

        async for message in ws:
            try:
                envelope = json.loads(message)
            except json.JSONDecodeError:
                continue

            request_id = envelope.get("request_id")
            command = {"type": envelope.get("type"), "params": envelope.get("params", {})}

            try:
                if blender_writer is None or blender_writer.is_closing():
                    blender_reader, blender_writer = await asyncio.open_connection(blender_host, blender_port)
                    logger.info(f"Connected to local Blender addon at {blender_host}:{blender_port}")

                blender_writer.write(json.dumps(command).encode("utf-8"))
                await blender_writer.drain()
                response_bytes = await _recv_full_json(blender_reader)
                response = json.loads(response_bytes.decode("utf-8"))
            except Exception as e:
                logger.error(f"Error talking to local Blender ({blender_host}:{blender_port}): {e}")
                response = {
                    "status": "error",
                    "message": (
                        f"Bridge agent could not reach Blender at {blender_host}:{blender_port}: {e}. "
                        "Make sure Blender is open, the addon is installed, and you clicked "
                        "'Connect to Claude' in the BlenderMCP sidebar tab."
                    ),
                }
                blender_writer = None  # Force a fresh connection next time

            response["request_id"] = request_id
            await ws.send(json.dumps(response))


async def main_async(relay_url: str, key: str, blender_host: str, blender_port: int):
    while True:
        try:
            await _run_session(relay_url, key, blender_host, blender_port)
        except Exception as e:
            logger.warning(f"Relay connection lost ({e}). Reconnecting in {RECONNECT_DELAY}s...")
        await asyncio.sleep(RECONNECT_DELAY)


def main():
    parser = argparse.ArgumentParser(description="BlenderMCP local bridge agent")
    parser.add_argument(
        "--relay-url",
        default=os.getenv("BLENDER_BRIDGE_RELAY_URL"),
        help="wss:// URL of the hosted BlenderMCP server's /agent endpoint",
    )
    parser.add_argument(
        "--key",
        default=os.getenv("BLENDER_BRIDGE_KEY"),
        help="Your personal key -- must match the blender_key used in your MCP server URL",
    )
    parser.add_argument("--blender-host", default=os.getenv("BLENDER_HOST", "localhost"))
    parser.add_argument("--blender-port", type=int, default=int(os.getenv("BLENDER_PORT", "9876")))
    args = parser.parse_args()

    if not args.relay_url or not args.key:
        parser.error(
            "--relay-url and --key are required "
            "(or set BLENDER_BRIDGE_RELAY_URL / BLENDER_BRIDGE_KEY)"
        )

    logger.info(f"Starting bridge agent for key '{args.key}'")
    try:
        asyncio.run(main_async(args.relay_url, args.key, args.blender_host, args.blender_port))
    except KeyboardInterrupt:
        logger.info("Stopped.")


if __name__ == "__main__":
    main()
