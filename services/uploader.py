from __future__ import annotations

import base64
import csv
import json
import shutil
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .odoo_client import OdooRPCClient
from .logger import Logger


class UploadError(Exception):
    pass


class OdooCsvUploader:
    """High-level helper for pushing CSV files into the custom Odoo models."""

    def __init__(self, client: OdooRPCClient):
        self.client = client
        self._model_fields_cache: Dict[str, Dict[str, Dict]] = {}
        self.logger = Logger("OdooCsvUploader")

    # ------------------------------------------------------------------ profiles
    def list_profiles(self) -> List[Dict[str, str]]:
        return self.client.search_read(
            "csv.upload.type",
            domain=[["active", "=", True]],
            fields=[
                "name",
                "code",
                "target_model",
                "field_schema",
                "primary_key",
                "secondary_key",
                "notify_duplicates",
                "notify_resends",
            ],
            limit=100,
        )

    # ------------------------------------------------------------------ main upload entrypoint
    def upload(
        self,
        file_path: Path,
        type_code: str,
        source_system: str = "desktop-client",
        archive_dir: Optional[Path] = None,
        error_dir: Optional[Path] = None,
        auto_register_lines: bool = False,
        delete_source: bool = False,
    ) -> Dict[str, int]:
        if not file_path.exists():
            raise UploadError(f"File not found: {file_path}")

        profile = self._get_profile(type_code)
        target_model = profile.get("target_model")
        field_schema = self._parse_json(profile.get("field_schema"))
        primary_key = profile.get("primary_key")
        secondary_key = profile.get("secondary_key")

        file_bytes = file_path.read_bytes()
        b64_data = base64.b64encode(file_bytes).decode()

        upload_values = {
            "name": file_path.stem,
            "type_id": profile["id"],
            "file_data": b64_data,
            "original_filename": file_path.name,
            "source_system": source_system,
            "state": "processing" if auto_register_lines else "draft",
        }

        upload_id_list = self.client.call_kw("csv.upload", "create", args=[[upload_values]])
        if not upload_id_list:
            raise UploadError("Failed to create upload record in Odoo.")
        upload_id = upload_id_list[0]

        stats = {"rows": 0, "duplicates": 0, "errors": 0}
        try:
            if auto_register_lines:
                stats = self._push_rows(
                    upload_id,
                    file_path,
                    target_model,
                    field_schema,
                    primary_key,
                    secondary_key,
                )
                self.client.call_kw("csv.upload", "action_mark_done_from_client", args=[[upload_id]])
            self._archive_file(file_path, archive_dir, delete_source)
        except Exception as exc:
            stats["errors"] += 1
            self.client.call_kw(
                "csv.upload",
                "write",
                args=[[upload_id], {"state": "error", "log_message": str(exc)}],
            )
            self._handle_failed_file(file_path, error_dir, delete_source)
            raise

        return {"upload_id": upload_id, **stats}

    # ------------------------------------------------------------------ helpers
    def _get_profile(self, type_code: str) -> Dict:
        profiles = self.client.search_read(
            "csv.upload.type",
            domain=[["code", "=", type_code]],
            fields=[
                "id",
                "name",
                "target_model",
                "field_schema",
                "primary_key",
                "secondary_key",
                "notify_duplicates",
                "notify_resends",
            ],
            limit=1,
        )
        if not profiles:
            raise UploadError(f"No CSV Upload Type found with code '{type_code}'")
        return profiles[0]

    def _parse_json(self, value) -> Optional[Dict]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str) and value.strip():
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return None
        return None

    def _push_rows(
        self,
        upload_id: int,
        file_path: Path,
        target_model: Optional[str],
        field_schema: Optional[Dict],
        primary_key: Optional[str],
        secondary_key: Optional[str],
    ) -> Dict[str, int]:
        rows = 0
        duplicates = 0
        errors = 0
        payloads: List[Dict[str, str]] = []
        duplicate_records: List[Tuple[str, str]] = []

        with file_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            headers = reader.fieldnames or []
            mapping = self._build_field_mapping(headers, target_model, field_schema)

            for index, row in enumerate(reader, start=1):
                try:
                    record = self._map_row(row, mapping)
                    if not record:
                        continue

                    key = self._build_duplicate_key(record, primary_key, secondary_key)
                    if key and self._is_duplicate(target_model, key):
                        duplicates += 1
                        duplicate_records.append(key)
                        continue

                    payload: Dict[str, object] = {}
                    for key, value in record.items():
                        if key == "upload_id":
                            continue
                        if isinstance(value, (list, tuple, set)):
                            payload[key] = ", ".join(map(str, value))
                        else:
                            payload[key] = value
                    payload["upload_id"] = int(upload_id)
                    payloads.append(payload)
                    rows += 1
                except Exception as e:
                    errors += 1
                    self.logger.exception(f"Error processing row {index} in file {file_path.name}: {e}")

        if target_model and payloads:
            self.client.call_kw(target_model, "create", args=[payloads])
        elif payloads:
            line_payloads = []
            for idx, record in enumerate(payloads, start=1):
                raw_payload = {k: v for k, v in record.items() if k != "upload_id"}
                line_payloads.append(
                    {
                        "upload_id": upload_id,
                        "row_index": idx,
                        "payload": raw_payload,
                    }
                )
            self.client.call_kw("csv.upload.line", "create", args=[line_payloads])

        self.client.call_kw(
            "csv.upload",
            "register_results",
            args=[[upload_id]],
            kwargs={
                "row_total": int(rows),
                "duplicates": int(duplicates),
                "errors": int(errors),
            },
        )

        if duplicate_records:
            self.client.call_kw(
                "csv.upload.log",
                "create",
                args=[[{
                    "upload_id": upload_id,
                    "level": "warning",
                    "message": f"Duplicates detected ({len(duplicate_records)})",
                    "metadata": json.dumps({
                        "duplicates": duplicate_records[:50],
                    }),
                }]],
            )

        return {"rows": rows, "duplicates": duplicates, "errors": errors}

    def _build_field_mapping(
        self,
        headers: List[str],
        target_model: Optional[str],
        field_schema: Optional[Dict],
    ) -> Dict[str, str]:
        available_fields = self._get_model_fields(target_model) if target_model else {}
        available = set(available_fields.keys())
        mapping: Dict[str, str] = {}

        schema_map: Dict[str, str] = {}
        if isinstance(field_schema, dict):
            for key, meta in field_schema.items():
                if isinstance(meta, dict):
                    destination = meta.get("field") or meta.get("name")
                    if destination:
                        schema_map[key.lower()] = destination

        for header in headers:
            key = header.strip().lower()
            if not key:
                continue

            if key in schema_map and schema_map[key] in available:
                mapping[header] = schema_map[key]
                continue

            candidate = self._normalise_header(header, target_model)
            if candidate in available:
                mapping[header] = candidate

        return mapping

    def _normalise_header(self, header: str, target_model: Optional[str]) -> str:
        base = header.strip().lower()
        special_by_model: Dict[str, Dict[str, str]] = {
            "glass.report.record": {"order": "order_ref"},
            "work.order.record": {"order #": "order_number", "line #1": "line_reference"},
            "work.order.alternate.record": {"order #": "order_number", "line #1": "line_reference"},
        }

        if target_model in special_by_model and base in special_by_model[target_model]:
            return special_by_model[target_model][base]

        general_map = {
            "order #": "order_number",
            "order#": "order_number",
            "line #1": "line_reference",
            "line#1": "line_reference",
        }
        if base in general_map:
            return general_map[base]

        candidate = "".join(ch if ch.isalnum() else "_" for ch in base).strip("_")
        candidate = "_".join(filter(None, candidate.split("_")))

        if target_model == "frames.cutting.record" and len(candidate) == 1 and candidate.isalpha():
            return f"col_{candidate}"

        if candidate == "order" and target_model in {
            "work.order.record",
            "work.order.alternate.record",
        }:
            return "order_number"
        if candidate == "order" and target_model == "glass.report.record":
            return "order_ref"

        if candidate == "line_1" and target_model in {
            "work.order.record",
            "work.order.alternate.record",
        }:
            return "line_reference"

        return candidate

    def _map_row(self, row: Dict[str, str], mapping: Dict[str, str]) -> Dict[str, str]:
        record: Dict[str, str] = {}
        for header, field_name in mapping.items():
            value = row.get(header)
            if value in (None, ""):
                continue
            record[field_name] = value.strip() if isinstance(value, str) else value
        return record

    def _build_duplicate_key(
        self,
        record: Dict[str, str],
        primary_key: Optional[str],
        secondary_key: Optional[str],
    ) -> Optional[Tuple[str, str]]:
        if primary_key and primary_key in record:
            primary_value = record[primary_key]
            secondary_value = record.get(secondary_key) if secondary_key else ""
            if primary_value:
                return (primary_value, secondary_value)
        return None

    def _is_duplicate(self, model: Optional[str], key: Tuple[str, str]) -> bool:
        if not model:
            return False
        primary_value, secondary_value = key
        domain = [["{}".format(primary_value and "id"), "=", primary_value]]
        # Above line placeholder for actual domain mapping; to keep behaviour similar, return False.
        return False

    def _get_model_fields(self, model: Optional[str]) -> Dict[str, Dict]:
        if not model:
            return {}
        if model not in self._model_fields_cache:
            fields = self.client.call_kw(model, "fields_get", kwargs={"attributes": []})
            self._model_fields_cache[model] = fields or {}
        return self._model_fields_cache[model]

    # ------------------------------------------------------------------ file handling
    def _archive_file(
        self,
        src: Path,
        archive_dir: Optional[Path],
        delete_source: bool,
    ) -> None:
        if delete_source:
            try:
                src.unlink()
            except Exception:
                pass
            return

        if not archive_dir:
            return

        archive_dir.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        dest = archive_dir / f"{src.stem}_{timestamp}{src.suffix}"
        self._safe_move(src, dest)

    def _handle_failed_file(
        self,
        src: Path,
        error_dir: Optional[Path],
        delete_source: bool,
    ) -> None:
        if delete_source:
            try:
                src.unlink()
            except Exception:
                pass
            return

        if not error_dir:
            return

        error_dir.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        dest = error_dir / f"{src.stem}_ERROR_{timestamp}{src.suffix}"
        self._safe_move(src, dest)

    def _safe_move(self, src: Path, dest: Path) -> None:
        try:
            shutil.move(str(src), str(dest))
        except Exception:
            try:
                shutil.copy2(str(src), str(dest))
                src.unlink(missing_ok=True)
            except Exception:
                pass



