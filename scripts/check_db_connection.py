#!/usr/bin/env python3
"""
Prueba DATABASE_URL (lee .env vía Settings). No imprime usuario ni contraseña.

Uso desde la raíz del repo:
    python scripts/check_db_connection.py
"""
from __future__ import annotations

import asyncio
import socket
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    from sqlalchemy import text
    from sqlalchemy.engine.url import make_url

    from backend.app.core.config import settings
    from backend.app.core.db import engine

    try:
        u = make_url(settings.DATABASE_URL)
    except Exception as exc:
        print("ERROR: DATABASE_URL no se puede parsear:", exc)
        sys.exit(1)

    host = u.host or ""
    port = u.port or (5432 if "postgresql" in settings.DATABASE_URL else None)
    print("Driver:", u.drivername)
    print("Host:", host or "(vacío — revisa si la contraseña tiene @ sin codificar)")
    print("Puerto:", port)
    print("Base:", u.database)

    if not host:
        print("\nSin hostname la URL está rota (suele ser contraseña con @ : # sin quote_plus).")
        sys.exit(1)

    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        fams = {socket.AddressFamily(i[0]).name for i in infos}
        print("DNS OK, familias:", ", ".join(sorted(fams)))
    except socket.gaierror as exc:
        print("ERROR DNS (no resuelve el host):", exc)
        print("Comprueba typo, VPN/DNS o conexión.")
        if ".supabase.co" in host and host.startswith("db."):
            print(
                "\nNota: el host directo db.*.supabase.co suele ser solo IPv6. "
                "Si tu red o Python no usan IPv6, fallará aquí y también en asyncpg.\n"
                "Solución: en Supabase → Database → Connection string, elige el "
                "**Pooler** (Session o Transaction), host tipo "
                "aws-0-REGION.pooler.supabase.com, puerto 6543 (transacción) o el que indique el panel.\n"
                "DATABASE_URL sigue siendo postgresql+asyncpg://..."
            )
        sys.exit(1)

    async def ping() -> None:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

    try:
        asyncio.run(ping())
    except Exception as exc:
        print("ERROR al conectar con SQLAlchemy/asyncpg:", exc)
        print(
            "Si estás en Render: usa el connection string del POOLER (IPv4), "
            "no solo db.xxx.supabase.co directo."
        )
        sys.exit(1)

    print("Conexión y SELECT 1 OK.")


if __name__ == "__main__":
    main()
