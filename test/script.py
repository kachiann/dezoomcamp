from pathlib import Path  # Object-oriented file paths (Python 3.4+)

current_dir = Path.cwd()        # Gets absolute path of current working directory
current_file = Path(__file__).name  # Filename of this script (e.g., "script.py")

print(f"Files in {current_dir}:")  # Header with dir path

for filepath in current_dir.iterdir():  # Iterates over immediate children only
    if filepath.name == current_file:
        continue  # Skip self to avoid reading own source

    print(f"  - {filepath.name}")  # List filename

    if filepath.is_file():         # Check if regular file (not dir/symlink)
        content = filepath.read_text(encoding='utf-8')  # Read as UTF-8 string
        print(f"    Content: {content}")  # Dump full contents
