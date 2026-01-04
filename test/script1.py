from pathlib import Path
import sys

def list_dir_safe(path=Path.cwd(), skip_file=None, max_size_mb=1):
    for fp in path.iterdir():
        if skip_file and fp.name == skip_file:
            continue
        print(f"  - {fp.name}")
        if fp.is_file():
            try:
                size_mb = fp.stat().st_size / (1024**2)
                if size_mb > max_size_mb:
                    print(f"    [SKIPPED: {size_mb:.1f}MB]")
                    continue
                content = fp.read_text(encoding='utf-8', errors='replace')
                print(f"    Preview: {content[:200]}...")
            except (OSError, UnicodeDecodeError) as e:
                print(f"    [ERROR: {e}]")

if __name__ == "__main__":
    skip = Path(__file__).name if __file__ else None
    list_dir_safe(skip_file=skip)
