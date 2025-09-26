from __future__ import annotations

import xmlrpc.client
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence


class OdooRPCError(Exception):
    """Raised when an XML-RPC interaction with Odoo fails."""


@dataclass(frozen=True, slots=True)
class OdooConnectionDetails:
    url: str
    database: str
    username: str
    password: str


class OdooRPCClient:
    """Thin XML-RPC client tailored for desktop CSV upload workflows."""

    def __init__(
        self,
        details: OdooConnectionDetails,
        *,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.details = details
        base_url = details.url.rstrip("/")
        self._common = xmlrpc.client.ServerProxy(f"{base_url}/xmlrpc/2/common", allow_none=True)
        self._object = xmlrpc.client.ServerProxy(f"{base_url}/xmlrpc/2/object", allow_none=True)
        self.context: Dict[str, Any] = context or {}
        self._uid: Optional[int] = None

    # ------------------------------------------------------------------ auth helpers
    def authenticate(self) -> int:
        try:
            uid = self._common.authenticate(
                self.details.database,
                self.details.username,
                self.details.password,
                {}
            )
        except Exception as exc:  # pragma: no cover - network errors bubble up
            raise OdooRPCError(f"Authentication error: {exc}") from exc

        if not uid:
            raise OdooRPCError("Invalid Odoo credentials or database name")
        self._uid = int(uid)
        return self._uid

    def ensure_authenticated(self) -> int:
        return self._uid if self._uid is not None else self.authenticate()

    # ------------------------------------------------------------------ low level calls
    def call_kw(
        self,
        model: str,
        method: str,
        *,
        args: Optional[Sequence[Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Any:
        uid = self.ensure_authenticated()
        call_kwargs: Dict[str, Any] = dict(kwargs or {})
        call_context = context or self.context
        if call_context:
            call_kwargs.setdefault("context", call_context)

        try:
            return self._object.execute_kw(
                self.details.database,
                uid,
                self.details.password,
                model,
                method,
                list(args or []),
                call_kwargs,
            )
        except xmlrpc.client.Fault as fault:
            raise OdooRPCError(fault.faultString) from fault
        except Exception as exc:  # pragma: no cover - network errors bubble up
            raise OdooRPCError(f"RPC call failed: {exc}") from exc

    # ------------------------------------------------------------------ convenience wrappers
    def search_read(
        self,
        model: str,
        *,
        domain: Optional[Iterable[Any]] = None,
        fields: Optional[Sequence[str]] = None,
        limit: int = 0,
        offset: int = 0,
        order: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        kwargs: Dict[str, Any] = {
            "domain": list(domain or []),
            "fields": list(fields or []),
            "limit": limit,
        }
        if offset:
            kwargs["offset"] = offset
        if order:
            kwargs["order"] = order
        return self.call_kw(model, "search_read", args=[], kwargs=kwargs, context=context)

    def search_count(
        self,
        model: str,
        domain: Optional[Iterable[Any]] = None,
        *,
        context: Optional[Dict[str, Any]] = None,
    ) -> int:
        return int(self.call_kw(model, "search_count", args=[list(domain or [])], context=context))

    def read(
        self,
        model: str,
        ids: Sequence[int],
        fields: Optional[Sequence[str]] = None,
        *,
        context: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        return self.call_kw(model, "read", args=[list(ids), list(fields or [])], context=context)

    def write(
        self,
        model: str,
        ids: Sequence[int],
        values: Dict[str, Any],
        *,
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        return bool(self.call_kw(model, "write", args=[list(ids), values], context=context))

    def create(
        self,
        model: str,
        values: Dict[str, Any],
        *,
        context: Optional[Dict[str, Any]] = None,
    ) -> int:
        return int(self.call_kw(model, "create", args=[[values]], context=context))

    def test_connection(self) -> bool:
        try:
            self.ensure_authenticated()
            return True
        except OdooRPCError:
            raise
        except Exception as exc:  # pragma: no cover
            raise OdooRPCError(str(exc)) from exc
