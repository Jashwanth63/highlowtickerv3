"""SignalEmitter — async TCP server that pushes JSON-line signals to connected clients.

Usage:
    emitter = SignalEmitter(host="127.0.0.1", port=9137)
    await emitter.start()          # bind and begin accepting clients
    emitter.emit(signal_dict)      # non-blocking, called from _refresh_ui
    await emitter.stop()           # shut down
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from typing import Dict, Optional, Set


class SignalEmitter:
    """Lightweight TCP server that streams signal dicts as JSON lines.

    Design goals:
    - ``emit()`` is synchronous and never blocks the caller (Textual event loop).
    - Each client gets a ``Queue(maxsize=1)`` so only the latest signal is
      buffered; stale signals are silently dropped (latest-wins semantics).
    - Supports multiple simultaneous clients (e.g. several NinjaTrader strategies).
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 9137) -> None:
        self._host = host
        self._port = port
        self._server: Optional[asyncio.AbstractServer] = None
        self._clients: Dict[asyncio.Task, asyncio.Queue] = {}
        self._running = False

    async def start(self) -> None:
        """Bind the TCP server and begin accepting connections."""
        self._running = True
        self._server = await asyncio.start_server(
            self._on_client_connected, self._host, self._port,
        )
        addr = self._server.sockets[0].getsockname() if self._server.sockets else (self._host, self._port)
        print(f"[SignalEmitter] listening on {addr[0]}:{addr[1]}", file=sys.stderr)

    async def stop(self) -> None:
        """Shut down the server and disconnect all clients."""
        self._running = False
        # Cancel all client tasks
        for task in list(self._clients.keys()):
            task.cancel()
        self._clients.clear()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        print("[SignalEmitter] stopped", file=sys.stderr)

    def emit(self, signal: dict) -> None:
        """Enqueue a signal to all connected clients (non-blocking).

        If a client's queue is full (it hasn't consumed the previous signal),
        the old signal is replaced with the new one (latest-wins).
        """
        if not self._clients:
            return

        # Inject timestamp
        payload = dict(signal)
        payload["ts"] = time.time()
        line = json.dumps(payload, separators=(",", ":")) + "\n"
        encoded = line.encode("utf-8")

        for task, queue in list(self._clients.items()):
            if task.done():
                continue
            # Drain stale and put latest
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                queue.put_nowait(encoded)
            except asyncio.QueueFull:
                pass  # should not happen with maxsize=1 after drain

    @property
    def client_count(self) -> int:
        return sum(1 for t in self._clients if not t.done())

    # -- internals -----------------------------------------------------------

    async def _on_client_connected(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    ) -> None:
        """Callback when a new TCP client connects."""
        peer = writer.get_extra_info("peername")
        print(f"[SignalEmitter] client connected: {peer}", file=sys.stderr)
        queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=1)
        task = asyncio.current_task()
        # We can't use current_task for the key since this callback IS the task.
        # Instead, spawn a dedicated writer task.
        writer_task = asyncio.create_task(self._client_writer(writer, queue, peer))
        self._clients[writer_task] = queue

    async def _client_writer(
        self, writer: asyncio.StreamWriter, queue: asyncio.Queue, peer,
    ) -> None:
        """Dedicated task: drain the queue and write to the client socket."""
        try:
            while self._running:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    # Send a heartbeat so we detect broken connections
                    try:
                        writer.write(b"\n")
                        await writer.drain()
                    except (ConnectionError, OSError):
                        break
                    continue

                try:
                    writer.write(data)
                    await writer.drain()
                except (ConnectionError, OSError):
                    break
        except asyncio.CancelledError:
            pass
        finally:
            print(f"[SignalEmitter] client disconnected: {peer}", file=sys.stderr)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            # Remove self from clients dict
            task = asyncio.current_task()
            self._clients.pop(task, None)
