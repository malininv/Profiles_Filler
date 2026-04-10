"""CLI entry point for importing Profiles/*.txt into SQLite."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from profile_importer import load_config, run_import


def main() -> None:
    project_root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Импорт профилей из .txt в SQLite по import_rules.json",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Путь к SQLite (если не указан — берётся из rules db_path или Components.db в корне проекта)",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=project_root / "Profiles",
        help="Корень обхода .txt (по умолчанию папка Profiles)",
    )
    parser.add_argument(
        "--rules",
        type=Path,
        default=project_root / "import_rules.json",
        help="JSON с правилами column_map",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Не писать в БД, только отчёт",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Очистить затронутые таблицы без вопроса",
    )
    parser.add_argument(
        "--no-clear",
        action="store_true",
        help="Не очищать таблицы и не спрашивать",
    )
    args = parser.parse_args()

    if not args.rules.is_file():
        print(f"Rules file not found: {args.rules}", file=sys.stderr)
        sys.exit(1)
    if not args.root.is_dir():
        print(f"Root directory not found: {args.root}", file=sys.stderr)
        sys.exit(1)
    rules, cfg_db_path, _ = load_config(args.rules)
    db_path = args.db
    if db_path is None:
        db_path = Path(cfg_db_path) if cfg_db_path else (project_root / "Components.db")

    if not args.dry_run and not db_path.is_file():
        print(
            f"Database not found: {db_path}\n"
            "Создайте файл БД или укажите --db. Для прогона без БД используйте --dry-run.",
            file=sys.stderr,
        )
        sys.exit(1)

    run_import(
        db_path=db_path,
        root=args.root,
        rules_path=args.rules,
        dry_run=args.dry_run,
        clear_flag=args.clear,
        no_clear_flag=args.no_clear,
    )


if __name__ == "__main__":
    main()
