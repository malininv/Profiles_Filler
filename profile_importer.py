"""
Import .txt profile data into SQLite using import_rules.json.

The importer walks folders recursively under --root.
"""

from __future__ import annotations

import fnmatch
import json
import re
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping


@dataclass
class Rule:
    table: str
    skip_lines: int
    encoding: str
    column_map: dict[str, dict[str, Any]]
    numeric_columns: set[str]
    match: dict[str, Any]
    comment: str = ""
    encoding_try: tuple[str, ...] | None = None


def _strip_trailing_empty(parts: list[str]) -> list[str]:
    while parts and parts[-1] == "":
        parts.pop()
    return parts


def read_file_text(
    path: Path,
    encoding: str,
    encoding_try: tuple[str, ...] | None = None,
) -> str:
    raw = path.read_bytes()
    if encoding == "auto":
        order = encoding_try if encoding_try else (
            "utf-8-sig",
            "utf-8",
            "cp1251",
            "cp866",
            "koi8-r",
            "iso8859-5",
        )
        # Deterministic strategy: first encoding that decodes successfully.
        for enc in order:
            try:
                return raw.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return raw.decode("cp1251", errors="replace")
    try:
        return raw.decode(encoding)
    except UnicodeDecodeError:
        return raw.decode(encoding, errors="replace")

def read_json_text(path: Path) -> str:
    raw = path.read_bytes()
    for enc in ("utf-8-sig", "utf-8", "cp1251"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("cp1251", errors="replace")


def _rule_from_dict(obj: dict, label: str) -> Rule:
    cm = obj.get("column_map")
    if not isinstance(cm, dict) or not cm:
        raise ValueError(f"{label}: column_map required")
    nums = obj.get("numeric_columns") or []
    if not isinstance(nums, list):
        raise ValueError(f"{label}: numeric_columns must be a list")
    if "table" not in obj:
        raise ValueError(f"{label}: table required")
    et_raw = obj.get("encoding_try")
    encoding_try: tuple[str, ...] | None
    if et_raw is None:
        encoding_try = None
    elif isinstance(et_raw, list):
        encoding_try = tuple(str(x) for x in et_raw)
    else:
        raise ValueError(f"{label}: encoding_try must be a list of strings or omitted")
    return Rule(
        table=str(obj["table"]),
        skip_lines=int(obj.get("skip_lines", 0)),
        encoding=str(obj.get("encoding", "auto")),
        column_map=dict(cm),
        numeric_columns={str(x) for x in nums},
        match=dict(obj.get("match") or {}),
        comment=str(obj.get("comment", "")),
        encoding_try=encoding_try,
    )


def load_config(
    path: Path,
) -> tuple[list[Rule], str | None, dict[str, Any] | None]:
    data = json.loads(read_json_text(path))
    db_path = data.get("db_path")
    if db_path is not None and not isinstance(db_path, str):
        raise ValueError("db_path must be a string")

    po = data.get("profile_outlines")
    if po is not None and not isinstance(po, dict):
        raise ValueError("profile_outlines must be an object")

    top_cm = data.get("column_map")
    if isinstance(top_cm, dict) and top_cm:
        rule = _rule_from_dict(data, "import_rules.json (root)")
        return [rule], db_path, po

    rules_raw = data.get("rules")
    if not isinstance(rules_raw, list):
        raise ValueError("Config needs root-level column_map or a rules[] array")
    out: list[Rule] = []
    for i, obj in enumerate(rules_raw):
        if not isinstance(obj, dict):
            raise ValueError(f"rules[{i}] must be an object")
        out.append(_rule_from_dict(obj, f"rules[{i}]"))
    return out, db_path, po


def path_matches_rule(path: Path, root: Path, rule: Rule) -> bool:
    m = rule.match
    if not m:
        return True
    if "parent_folder" in m:
        if path.parent.name != m["parent_folder"]:
            return False
        return True
    if "path_glob" in m:
        try:
            rel = path.relative_to(root.resolve())
        except ValueError:
            return False
        rel_s = rel.as_posix()
        pat = m["path_glob"]
        if "/" not in pat and "\\" not in pat:
            return fnmatch.fnmatch(rel_s, pat) or fnmatch.fnmatch(path.name, pat)
        return fnmatch.fnmatch(rel_s, pat)
    return False


def find_rule(path: Path, root: Path, rules: list[Rule]) -> Rule | None:
    for r in rules:
        if path_matches_rule(path, root, r):
            return r
    return None


def max_required_field_index(column_map: dict[str, dict[str, Any]]) -> int:
    m = -1
    for spec in column_map.values():
        if spec.get("source") != "field_index":
            continue
        if spec.get("optional", False):
            continue
        idx = int(spec["index"])
        m = max(m, idx)
    return m


def normalize_cell(column: str, raw: str, numeric_columns: set[str]) -> str | float | None:
    s = raw.strip()
    if s == "":
        return None
    if column in numeric_columns:
        try:
            return float(s.replace(",", "."))
        except ValueError:
            return s
    return s


def build_row(
    *,
    rule: Rule,
    path: Path,
    header_line: str | None,
    parts: list[str],
    seq_state: dict[str, int],
    outline_folder_ids: dict[str, int] | None,
) -> dict[str, Any] | None:
    row: dict[str, Any] = {}
    for col, spec in rule.column_map.items():
        src = spec.get("source")
        if src == "header_line":
            row[col] = header_line if header_line is not None else ""
        elif src == "filename_stem":
            row[col] = path.stem
        elif src == "field_index":
            idx = int(spec["index"])
            optional = bool(spec.get("optional", False))
            if idx < 0 or idx >= len(parts):
                if optional:
                    row[col] = None
                    continue
                return None
            row[col] = normalize_cell(col, parts[idx], rule.numeric_columns)
        elif src == "constant":
            if "value" not in spec:
                raise ValueError(f"column {col!r}: constant source requires 'value'")
            row[col] = spec["value"]
        elif src == "sequence":
            if col not in seq_state:
                seq_state[col] = int(spec.get("start", 1))
            step = int(spec.get("step", 1))
            current = seq_state[col]
            seq_state[col] = current + step
            row[col] = current
        elif src == "outline_from_folder":
            if not outline_folder_ids:
                raise ValueError(
                    f"column {col!r}: outline_from_folder needs profile_outlines in JSON"
                )
            folder = path.parent.name
            oid = outline_folder_ids.get(folder)
            if oid is None:
                return None
            row[col] = oid
        else:
            raise ValueError(f"Unknown source: {src}")
    return row


def column_map_uses_source(rule: Rule, source: str) -> bool:
    return any(spec.get("source") == source for spec in rule.column_map.values())


def bootstrap_import_scope_sequences(rules: list[Rule], seq_state: dict[str, int]) -> None:
    for r in rules:
        for col, spec in r.column_map.items():
            if spec.get("source") != "sequence":
                continue
            if spec.get("scope", "import") != "import":
                continue
            if col not in seq_state:
                seq_state[col] = int(spec.get("start", 1))


def reset_file_scope_sequences(rule: Rule, seq_state: dict[str, int]) -> None:
    for col, spec in rule.column_map.items():
        if spec.get("source") != "sequence":
            continue
        if spec.get("scope", "import") == "file":
            seq_state[col] = int(spec.get("start", 1))


def _assert_safe_sql_ident(name: str) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise SystemExit(f"Invalid SQL identifier (use letters, digits, _): {name!r}")


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    _assert_safe_sql_ident(table)
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}


def validate_columns(conn: sqlite3.Connection, table: str, columns: list[str]) -> None:
    for c in columns:
        _assert_safe_sql_ident(c)
    existing = table_columns(conn, table)
    missing = [c for c in columns if c not in existing]
    if missing:
        raise SystemExit(
            f'Table "{table}" has no columns: {missing}. '
            f"Existing: {sorted(existing)}"
        )


def _resolve_config_path(p: str | None, config_dir: Path) -> Path | None:
    if not p:
        return None
    path = Path(p)
    if path.is_absolute():
        return path
    return (config_dir / path).resolve()


def _parse_outline_definitions(
    po: Mapping[str, Any],
) -> tuple[list[str], dict[str, dict[str, Any]]]:
    """
    Ordered outline names and per-name options: outline_symbol, outline_image_file.
    Use profile_outlines.outlines[] or legacy outline_names + outline_attributes.
    """
    per_name: dict[str, dict[str, Any]] = {}
    order: list[str] = []

    rows = po.get("outlines")
    if isinstance(rows, list) and len(rows) > 0:
        for i, item in enumerate(rows):
            if not isinstance(item, dict):
                raise ValueError(f"profile_outlines.outlines[{i}] must be an object")
            raw_name = item.get("outline_name")
            if raw_name is None or str(raw_name).strip() == "":
                raise ValueError(
                    f"profile_outlines.outlines[{i}]: outline_name is required"
                )
            n = str(raw_name)
            order.append(n)
            per_name[n] = {k: v for k, v in item.items() if k != "outline_name"}
    else:
        names_raw = po.get("outline_names")
        if not isinstance(names_raw, list) or not names_raw:
            raise ValueError(
                "profile_outlines: set non-empty outlines[] or outline_names[]"
            )
        order = [str(x) for x in names_raw]

    attrs = po.get("outline_attributes")
    if isinstance(attrs, dict):
        for k, v in attrs.items():
            if isinstance(v, dict):
                key = str(k)
                base = dict(per_name.get(key, {}))
                base.update(v)
                per_name[key] = base

    return order, per_name


def _symbol_for_outline(
    po: Mapping[str, Any], outline_name: str, per_name: dict[str, dict[str, Any]]
) -> str:
    row = per_name.get(outline_name, {})
    if "outline_symbol" in row:
        v = row["outline_symbol"]
        return "" if v is None else str(v)
    return str(po.get("outline_symbol_default", ""))


def _image_blob_for_outline(
    po: Mapping[str, Any],
    outline_name: str,
    config_dir: Path,
    global_blob: bytes | None,
    per_name: dict[str, dict[str, Any]],
) -> bytes | None:
    row = per_name.get(outline_name, {})
    path_raw = row.get("outline_image_file")
    if path_raw is not None and str(path_raw).strip() != "":
        ip = _resolve_config_path(str(path_raw), config_dir)
        if ip is None or not ip.is_file():
            raise SystemExit(
                f"outline_image_file for outline {outline_name!r} not found: {path_raw!r}"
            )
        return ip.read_bytes()
    return global_blob


def ensure_profile_outlines(
    conn: sqlite3.Connection,
    po: Mapping[str, Any],
    config_dir: Path,
) -> dict[str, int]:
    table = str(po.get("table", "ProfileOutlines"))
    id_col = str(po.get("id_column", "id"))
    name_col = str(po.get("name_column", "outline_name"))
    for ident in (table, id_col, name_col):
        _assert_safe_sql_ident(ident)

    sym_col = po.get("outline_symbol_column")
    if sym_col is not None:
        _assert_safe_sql_ident(str(sym_col))
    img_col = po.get("outline_image_column")
    if img_col is not None:
        _assert_safe_sql_ident(str(img_col))

    global_image: bytes | None = None
    img_path_cfg = po.get("outline_image_file")
    if img_path_cfg:
        ip = _resolve_config_path(str(img_path_cfg), config_dir)
        if ip is None or not ip.is_file():
            raise SystemExit(
                f"profile_outlines.outline_image_file not found: {img_path_cfg!r}"
            )
        global_image = ip.read_bytes()

    order, per_meta = _parse_outline_definitions(po)

    name_to_id: dict[str, int] = {}
    for n in order:
        cur = conn.execute(
            f"SELECT {id_col} FROM {table} WHERE {name_col} = ?",
            (n,),
        )
        row = cur.fetchone()
        if row is not None:
            name_to_id[n] = int(row[0])
            continue

        cols = [name_col]
        vals: list[Any] = [n]
        if sym_col:
            cols.append(str(sym_col))
            vals.append(_symbol_for_outline(po, n, per_meta))
        if img_col:
            cols.append(str(img_col))
            vals.append(_image_blob_for_outline(po, n, config_dir, global_image, per_meta))

        ph = ",".join("?" * len(vals))
        conn.execute(
            f"INSERT INTO {table} ({','.join(cols)}) VALUES ({ph})",
            vals,
        )
        cur2 = conn.execute(
            f"SELECT {id_col} FROM {table} WHERE {name_col} = ?",
            (n,),
        )
        row2 = cur2.fetchone()
        if row2 is None:
            raise SystemExit(f'Failed to read new row for outline_name={n!r} in {table}')
        name_to_id[n] = int(row2[0])

    return name_to_id


def build_outline_folder_ids(
    po: Mapping[str, Any], name_to_id: dict[str, int]
) -> dict[str, int]:
    fm = po.get("folder_to_outline")
    if not isinstance(fm, dict) or not fm:
        raise ValueError("profile_outlines.folder_to_outline must be a non-empty object")
    out: dict[str, int] = {}
    for folder, oname in fm.items():
        key = str(folder)
        val = str(oname)
        if val not in name_to_id:
            raise SystemExit(
                f"folder_to_outline[{key!r}] -> {val!r} has no matching outline "
                f"(check outlines[] / outline_names and spelling)"
            )
        out[key] = name_to_id[val]
    return out


def filter_jobs_by_outline_folders(
    jobs: list[tuple[Path, Rule]],
    folder_ids: dict[str, int],
    root: Path,
) -> list[tuple[Path, Rule]]:
    out: list[tuple[Path, Rule]] = []
    root = root.resolve()
    for path, rule in jobs:
        if not column_map_uses_source(rule, "outline_from_folder"):
            out.append((path, rule))
            continue
        folder = path.parent.name
        if folder not in folder_ids:
            print(
                f"skip (no outline mapping for folder {folder!r}): "
                f"{path.relative_to(root)}",
                file=sys.stderr,
            )
            continue
        out.append((path, rule))
    return out


def iter_txt_files(root: Path) -> Iterator[Path]:
    root = root.resolve()
    for p in root.rglob("*.txt"):
        if "venv" in p.parts:
            continue
        yield p


def collect_jobs(
    root: Path, rules: list[Rule]
) -> tuple[list[tuple[Path, Rule]], set[str]]:
    jobs: list[tuple[Path, Rule]] = []
    tables: set[str] = set()
    root = root.resolve()
    for path in iter_txt_files(root):
        rule = find_rule(path, root, rules)
        if rule is None:
            print(f"skip (no matching rule): {path.relative_to(root)}", file=sys.stderr)
            continue
        jobs.append((path, rule))
        tables.add(rule.table)
    return jobs, tables


def parse_data_lines(text: str, skip_lines: int) -> tuple[str | None, list[str]]:
    lines = text.splitlines()
    header: str | None = None
    if skip_lines > 0:
        head = lines[:skip_lines]
        rest = lines[skip_lines:]
        if head:
            header = head[0].strip()
    else:
        rest = lines
    return header, rest


def decide_clear(
    *,
    clear_flag: bool,
    no_clear_flag: bool,
    stdin_is_tty: bool,
    tables: set[str],
) -> bool:
    if clear_flag and no_clear_flag:
        raise SystemExit("Use only one of --clear or --no-clear")
    if clear_flag:
        return True
    if no_clear_flag:
        return False
    if not stdin_is_tty:
        print(
            "Non-interactive stdin: not clearing tables (use --clear or --no-clear).",
            file=sys.stderr,
        )
        return False
    names = ", ".join(sorted(tables))
    prompt = f"\u041e\u0447\u0438\u0441\u0442\u0438\u0442\u044c \u0442\u0430\u0431\u043b\u0438\u0446\u044b \u043f\u0435\u0440\u0435\u0434 \u0438\u043c\u043f\u043e\u0440\u0442\u043e\u043c? [{names}] (y/N): "
    ans = input(prompt).strip().lower()
    return ans in ("y", "yes", "\u0434", "\u0434\u0430")


def run_import(
    *,
    db_path: Path,
    root: Path,
    rules_path: Path,
    dry_run: bool,
    clear_flag: bool,
    no_clear_flag: bool,
) -> None:
    rules, _, po = load_config(rules_path)
    jobs, tables = collect_jobs(root, rules)
    if not jobs:
        print("No .txt files found under root (or all skipped).", file=sys.stderr)
        return

    needs_outline_ids = any(
        column_map_uses_source(r, "outline_from_folder") for r in rules
    )
    if needs_outline_ids and po is None:
        raise SystemExit(
            "column_map uses outline_from_folder: add a root-level "
            '"profile_outlines" block to import_rules.json'
        )

    if dry_run:
        if clear_flag and no_clear_flag:
            raise SystemExit("Use only one of --clear or --no-clear")
        would_clear = clear_flag
        print(f"[dry-run] Would process {len(jobs)} files into tables: {sorted(tables)}")
        if po is not None and needs_outline_ids:
            try:
                seed_order, _ = _parse_outline_definitions(po)
                n_seeds = len(seed_order)
            except ValueError:
                n_seeds = 0
            print(
                f"[dry-run] Would ensure up to {n_seeds} row(s) in "
                f'{po.get("table", "ProfileOutlines")} (missing outline_name only)'
            )
        if would_clear:
            print(f"[dry-run] Would DELETE FROM: {sorted(tables)}")
        elif not clear_flag and not no_clear_flag and sys.stdin.isatty():
            print(
                "[dry-run] On a real run you will be asked to clear tables "
                "(or pass --clear / --no-clear).",
                file=sys.stderr,
            )
        return

    do_clear = decide_clear(
        clear_flag=clear_flag,
        no_clear_flag=no_clear_flag,
        stdin_is_tty=sys.stdin.isatty(),
        tables=tables,
    )

    conn = sqlite3.connect(str(db_path))
    try:
        outline_folder_ids: dict[str, int] | None = None
        if po is not None and needs_outline_ids:
            ot = str(po.get("table", "ProfileOutlines"))
            id_c = str(po.get("id_column", "id"))
            nc = str(po.get("name_column", "outline_name"))
            oc_needed = [id_c, nc]
            if po.get("outline_symbol_column"):
                oc_needed.append(str(po["outline_symbol_column"]))
            if po.get("outline_image_column"):
                oc_needed.append(str(po["outline_image_column"]))
            validate_columns(conn, ot, oc_needed)
            name_to_id = ensure_profile_outlines(conn, po, rules_path.parent)
            folder_ids = build_outline_folder_ids(po, name_to_id)
            jobs = filter_jobs_by_outline_folders(jobs, folder_ids, root)
            if not jobs:
                print(
                    "No .txt files left after profile_outlines folder filter.",
                    file=sys.stderr,
                )
                return
            tables = {r.table for _, r in jobs}
            outline_folder_ids = folder_ids

        table_cols: dict[str, set[str]] = {}
        for _, r in jobs:
            table_cols.setdefault(r.table, set()).update(r.column_map.keys())
        for t in sorted(tables):
            validate_columns(conn, t, sorted(table_cols[t]))

        if do_clear:
            for t in sorted(tables):
                _assert_safe_sql_ident(t)
                conn.execute(f"DELETE FROM {t}")
            print(f"Cleared tables: {sorted(tables)}")

        seq_state: dict[str, int] = {}
        bootstrap_import_scope_sequences([r for _, r in jobs], seq_state)

        total_rows = 0
        for path, rule in jobs:
            reset_file_scope_sequences(rule, seq_state)
            text = read_file_text(path, rule.encoding, rule.encoding_try)
            header, data_lines = parse_data_lines(text, rule.skip_lines)
            need_header = any(
                spec.get("source") == "header_line"
                for spec in rule.column_map.values()
            )
            if need_header and rule.skip_lines < 1:
                header = header or ""

            max_idx = max_required_field_index(rule.column_map)
            insert_cols = list(rule.column_map.keys())
            for c in insert_cols:
                _assert_safe_sql_ident(c)
            placeholders = ",".join("?" * len(insert_cols))
            col_list = ",".join(insert_cols)
            qmarks = f"INSERT INTO {rule.table} ({col_list}) VALUES ({placeholders})"

            batch: list[tuple[Any, ...]] = []
            skipped = 0
            for line in data_lines:
                line = line.strip()
                if not line:
                    continue
                parts = [p.strip() for p in line.split(";")]
                parts = _strip_trailing_empty(parts)
                if max_idx >= len(parts):
                    skipped += 1
                    continue
                row = build_row(
                    rule=rule,
                    path=path,
                    header_line=header,
                    parts=parts,
                    seq_state=seq_state,
                    outline_folder_ids=outline_folder_ids,
                )
                if row is None:
                    skipped += 1
                    continue
                batch.append(tuple(row[c] for c in insert_cols))

            if batch:
                conn.executemany(qmarks, batch)
                total_rows += len(batch)
            rel = path.relative_to(root.resolve())
            if skipped:
                print(f"{rel}: inserted {len(batch)}, skipped lines {skipped}")
            else:
                print(f"{rel}: inserted {len(batch)}")

        conn.commit()
        print(f"Done. Total rows inserted: {total_rows}")
    finally:
        conn.close()
