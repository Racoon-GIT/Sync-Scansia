"""An offline stand-in for ``mysql.connector.connect`` that still runs every
statement through the DRIVER'S OWN parameter substitution.

``SVILUPPO/docs/shared-database.md``: "A test feeding a mocked cursor never
substitutes and cannot catch this [a literal ``%`` eating a bound parameter]:
exercise such a statement through the driver's own substitution." Each
:class:`OfflineCursor` hands the statement to a real ``MySQLCursor`` bound to a
never-connected ``MySQLConnection`` subclass, which captures the exact bytes the
driver would send; a test-supplied handler then decides the result (rowcount,
lastrowid, rows) from that final SQL text.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pytest

mysql_connector = pytest.importorskip("mysql.connector")

from mysql.connector.connection import MySQLConnection  # noqa: E402
from mysql.connector.conversion import MySQLConverter  # noqa: E402
from mysql.connector.cursor import MySQLCursor  # noqa: E402


class _Substituter(MySQLConnection):
    """Never connects: captures the statement the driver builds."""

    def __init__(self) -> None:
        super().__init__()
        self.converter = MySQLConverter("utf8mb4", True)
        self.sent: List[bytes] = []

    @property
    def sql_mode(self) -> str:  # the real property asks the server
        return ""

    def handle_unread_result(self, *a: Any, **k: Any) -> None:
        return None

    def cmd_query(self, stmt: Any, *a: Any, **k: Any) -> Dict[str, Any]:
        self.sent.append(stmt)
        return {"affected_rows": 0, "insert_id": 0, "warning_count": 0,
                "server_status": 0, "info_msg": ""}


def substitute(sql: str, params: Sequence[Any] = ()) -> str:
    """The final statement text the driver would send (raises its own
    ``ProgrammingError`` on a placeholder / parameter mismatch)."""
    conn = _Substituter()
    MySQLCursor(conn).execute(sql, tuple(params) if params else None)
    return conn.sent[-1].decode("utf-8")


Result = Tuple[int, Optional[int], List[Tuple[Any, ...]]]  # rowcount, lastrowid, rows
Handler = Callable[[str, "OfflineConnection"], Result]


class OfflineCursor:
    def __init__(self, conn: "OfflineConnection") -> None:
        self.conn = conn
        self.rowcount = -1
        self.lastrowid: Optional[int] = None
        self._rows: List[Tuple[Any, ...]] = []

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        if self.conn.closed:
            raise mysql_connector.errors.OperationalError(errno=2055, msg="Lost connection")
        final = substitute(sql, params)
        self.conn.log.append(final)
        fail_on = self.conn.driver.fail_on
        if fail_on is not None:
            exc = fail_on(final)
            if exc is not None:
                raise exc
        self.rowcount, self.lastrowid, self._rows = self.conn.handler(final, self.conn)

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> List[Tuple[Any, ...]]:
        return list(self._rows)

    def close(self) -> None:
        return None


class OfflineConnection:
    def __init__(self, driver: "OfflineDriver", kwargs: Dict[str, Any]) -> None:
        self.driver = driver
        self.kwargs = kwargs
        self.autocommit = kwargs.get("autocommit", False)
        self.closed = False
        self.log: List[str] = []
        self.handler = driver.handler
        self.commits = 0
        self.rollbacks = 0
        self.transactions = 0
        self.txn_kwargs: List[Dict[str, Any]] = []

    def cursor(self) -> OfflineCursor:
        return OfflineCursor(self)

    def start_transaction(self, **kw: Any) -> None:
        if self.closed:
            raise mysql_connector.errors.OperationalError(errno=2055, msg="Lost connection")
        self.transactions += 1
        self.txn_kwargs.append(kw)
        self.log.append("START TRANSACTION")

    def commit(self) -> None:
        if self.driver.fail_commit:
            raise mysql_connector.errors.OperationalError(errno=2013, msg="Lost connection during query")
        self.commits += 1
        self.log.append("COMMIT")

    def rollback(self) -> None:
        self.rollbacks += 1
        self.log.append("ROLLBACK")

    def close(self) -> None:
        self.closed = True


class OfflineDriver:
    """``connect=driver.connect``; every connection shares ``handler``/``fail_on``."""

    def __init__(self, handler: Handler, *, database: str = "racoon_staging", autocommit_reply: int = 1) -> None:
        self.database = database
        self.autocommit_reply = autocommit_reply
        self.user_handler = handler
        self.fail_on: Optional[Callable[[str], Optional[BaseException]]] = None
        self.fail_commit = False
        self.connections: List[OfflineConnection] = []

    def handler(self, sql: str, conn: OfflineConnection) -> Result:
        if sql.startswith("SET SESSION"):
            return 0, None, []
        if sql.startswith("SELECT @@SESSION.autocommit"):
            return 1, None, [(self.autocommit_reply, self.database)]
        return self.user_handler(sql, conn)

    def connect(self, **kwargs: Any) -> OfflineConnection:
        conn = OfflineConnection(self, kwargs)
        self.connections.append(conn)
        return conn

    @property
    def statements(self) -> List[str]:
        return [s for c in self.connections for s in c.log]


def driver_error(errno: int, kind: str = "DatabaseError") -> BaseException:
    cls = getattr(mysql_connector.errors, kind)
    return cls(errno=errno, msg=f"simulated {errno}")


__all__ = ["substitute", "OfflineDriver", "OfflineConnection", "driver_error"]
