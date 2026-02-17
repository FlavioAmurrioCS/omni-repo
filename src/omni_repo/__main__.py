from __future__ import annotations


def main(argv: list[str] | None = None) -> int:
    from omni_repo.main import app

    app(argv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
