import os
from pathlib import Path
import hashlib

def get_files_with_hashes(root_dir, ignore_dirs, ignore_extensions):
    file_map = {}
    root_path = Path(root_dir).resolve()
    
    for dirpath, dirnames, filenames in os.walk(root_path):
        # Filter out ignored directories
        dirnames[:] = [d for d in dirnames if d not in ignore_dirs and not d.startswith('.')]
        
        for f in filenames:
            ext = os.path.splitext(f)[1].lower()
            if ext in ignore_extensions or f.startswith('.'):
                continue
                
            # Skip specific log/temp files
            if f.endswith('.log') or f.endswith('_diff.txt') or 'runtime' in f or 'dump_' in f or 'temp_' in f or f == 'diff.txt' or 'post_rp' in f or 'scratch_' in f:
                continue
                
            full_path = Path(dirpath) / f
            rel_path = str(full_path.relative_to(root_path)).replace("\\", "/")
            
            # Skip if it's in a restore point folder
            if "restore_point_" in rel_path or "ut1-index" in rel_path or "scratch/" in rel_path or "archive/" in rel_path:
                continue
                
            try:
                # Read and hash
                with open(full_path, "rb") as file_obj:
                    file_hash = hashlib.md5(file_obj.read()).hexdigest()
                file_map[rel_path] = file_hash
            except Exception as e:
                pass
    return file_map

def compare():
    main_dir = "C:/Users/sagnik/Desktop/ut index 2"
    github_dir = "C:/Users/sagnik/Desktop/ut index 2/ut1-index-final3"
    
    ignore_dirs = {"__pycache__", "venv", ".git", "logs", "archive", "data_store", "empty_dir"}
    ignore_extensions = {".pyc", ".db", ".sqlite", ".log"}
    
    print("Gathering files from main workspace...")
    main_files = get_files_with_hashes(main_dir, ignore_dirs, ignore_extensions)
    
    print("Gathering files from github clone...")
    github_files = get_files_with_hashes(github_dir, ignore_dirs, ignore_extensions)
    
    missing_in_github = []
    different_content = []
    
    for rel_path, f_hash in main_files.items():
        if rel_path not in github_files:
            missing_in_github.append(rel_path)
        elif github_files[rel_path] != f_hash:
            different_content.append(rel_path)
            
    print("\n--- RESULTS ---")
    if not missing_in_github and not different_content:
        print("SUCCESS! The GitHub version has all important files from the main system folder, and their contents match perfectly.")
    else:
        if missing_in_github:
            print(f"WARNING: {len(missing_in_github)} files are present in the main system but MISSING in the GitHub version:")
            for m in sorted(missing_in_github):
                print(f"  - {m}")
                
        if different_content:
            print(f"\nWARNING: {len(different_content)} files have DIFFERENT content in the GitHub version:")
            for d in sorted(different_content):
                print(f"  - {d}")

if __name__ == "__main__":
    compare()
