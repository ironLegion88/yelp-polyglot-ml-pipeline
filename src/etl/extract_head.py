import os
from pathlib import Path

def extract_head(source_dir: Path, target_dir: Path, n_lines: int = 5000):
    """
    Extracts the first `n_lines` from each .json file in `source_dir`
    and saves them under `name_head.json` in `target_dir`.
    """
    # Create the target directory if it doesn't exist
    target_dir.mkdir(parents=True, exist_ok=True)

    # Find all matching JSON files in the source directory
    # excluding the target directory itself if it's nested
    json_files = [f for f in source_dir.glob("*.json") if f.is_file()]

    if not json_files:
        print(f"No JSON files found in {source_dir}")
        return

    for file_path in json_files:
        target_file_path = target_dir / f"{file_path.name}_head.json"
        print(f"Processing {file_path.name} -> {target_file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as infile, \
                 open(target_file_path, 'w', encoding='utf-8') as outfile:
                
                for i, line in enumerate(infile):
                    if i >= n_lines:
                        break
                    outfile.write(line)
                    
            print(f"  Saved {min(i, n_lines)} lines.")
        except Exception as e:
            print(f"  Error processing {file_path.name}: {e}")

if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent.parent
    
    raw_data_dir = project_root / "data" / "raw"
    head_data_dir = raw_data_dir / "head"
    
    extract_head(raw_data_dir, head_data_dir, n_lines=5000)
    
    print("\nExtraction complete!")