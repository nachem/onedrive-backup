#!/usr/bin/env python3
"""
Demonstration of Change Detection and Duplicate Prevention

This script demonstrates how the backup system prevents duplicate uploads
by tracking file states and only uploading changed files.
"""

import sys
import tempfile
import json
from pathlib import Path
from datetime import datetime, timedelta

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from onedrive_backup.sync.file_tracker import FileTracker, FileInfo

def demo_change_detection():
    """Demonstrate the change detection system."""
    print("🔍 OneDrive Backup Change Detection Demo")
    print("=" * 60)
    
    # Create a temporary tracker file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
        tracker_file = Path(tmp_file.name)
    
    try:
        # Initialize file tracker
        tracker = FileTracker(tracker_file)
        
        print("📋 Testing Change Detection Logic...")
        print()
        
        # Simulate some OneDrive files
        test_files = [
            {
                'path': '/OneDrive/Documents/report.pdf',
                'size': 1024000,
                'modified': datetime(2024, 10, 1, 10, 30, 0),
                'hash': 'abc123def456'
            },
            {
                'path': '/OneDrive/Pictures/vacation.jpg',
                'size': 2048000,
                'modified': datetime(2024, 10, 2, 14, 15, 0),
                'hash': 'xyz789uvw123'
            },
            {
                'path': '/OneDrive/Projects/code.py',
                'size': 5120,
                'modified': datetime(2024, 10, 3, 9, 45, 0),
                'hash': 'code456hash789'
            }
        ]
        
        # === SCENARIO 1: First backup (all files are new) ===
        print("📦 SCENARIO 1: First Backup - All Files Are New")
        print("-" * 50)
        
        files_to_backup = []
        
        for file_info in test_files:
            file_path = file_info['path']
            size = file_info['size']
            modified = file_info['modified']
            file_hash = file_info['hash']
            
            # Check if file has changed (should be True for new files)
            has_changed = tracker.has_file_changed(file_path, size, modified, file_hash)
            
            print(f"📄 {file_path}")
            print(f"   Size: {size:,} bytes")
            print(f"   Modified: {modified}")
            print(f"   Status: {'🆕 NEW FILE - Will backup' if has_changed else '✅ No changes - Skip'}")
            
            if has_changed:
                files_to_backup.append(file_info)
                # Simulate successful backup
                tracker.update_file_info(file_path, size, modified, file_hash, "azure_blob")
            
            print()
        
        print(f"🚀 First Backup: {len(files_to_backup)}/{len(test_files)} files uploaded")
        print(f"📊 Total data: {sum(f['size'] for f in files_to_backup):,} bytes")
        print()
        
        # Save tracker state
        tracker.save()
        
        # === SCENARIO 2: Second backup (no changes) ===
        print("📦 SCENARIO 2: Second Backup - No Files Changed")
        print("-" * 50)
        
        files_to_backup = []
        
        for file_info in test_files:
            file_path = file_info['path']
            size = file_info['size']
            modified = file_info['modified']
            file_hash = file_info['hash']
            
            # Check if file has changed (should be False - files haven't changed)
            has_changed = tracker.has_file_changed(file_path, size, modified, file_hash)
            
            print(f"📄 {file_path}")
            print(f"   Status: {'🔄 CHANGED - Will backup' if has_changed else '✅ No changes - Skip backup'}")
            
            if has_changed:
                files_to_backup.append(file_info)
            
            print()
        
        print(f"🚀 Second Backup: {len(files_to_backup)}/{len(test_files)} files uploaded")
        print(f"💡 Result: No uploads needed - all files are unchanged!")
        print()
        
        # === SCENARIO 3: Third backup (one file modified) ===
        print("📦 SCENARIO 3: Third Backup - One File Modified")
        print("-" * 50)
        
        # Modify one file
        test_files[1]['modified'] = datetime.now()  # vacation.jpg modified
        test_files[1]['size'] = 2100000  # Size increased
        test_files[1]['hash'] = 'new_hash_123'  # Content changed
        
        files_to_backup = []
        
        for file_info in test_files:
            file_path = file_info['path']
            size = file_info['size']
            modified = file_info['modified']
            file_hash = file_info['hash']
            
            # Check if file has changed
            has_changed = tracker.has_file_changed(file_path, size, modified, file_hash)
            
            stored_info = tracker.get_file_info(file_path)
            
            print(f"📄 {file_path}")
            if stored_info:
                print(f"   Previous size: {stored_info.size:,} bytes")
                print(f"   Current size: {size:,} bytes")
                print(f"   Previous modified: {stored_info.modified_time}")
                print(f"   Current modified: {modified}")
            
            print(f"   Status: {'🔄 CHANGED - Will backup' if has_changed else '✅ No changes - Skip'}")
            
            if has_changed:
                files_to_backup.append(file_info)
                # Update tracker after backup
                tracker.update_file_info(file_path, size, modified, file_hash, "azure_blob")
            
            print()
        
        print(f"🚀 Third Backup: {len(files_to_backup)}/{len(test_files)} files uploaded")
        print(f"📊 Data saved: Only {sum(f['size'] for f in files_to_backup):,} bytes uploaded (not {sum(f['size'] for f in test_files):,})")
        print()
        
        # === SCENARIO 4: Fourth backup (new file added) ===
        print("📦 SCENARIO 4: Fourth Backup - New File Added")
        print("-" * 50)
        
        # Add a new file
        new_file = {
            'path': '/OneDrive/Documents/new_document.docx',
            'size': 512000,
            'modified': datetime.now(),
            'hash': 'new_doc_hash_456'
        }
        
        test_files.append(new_file)
        files_to_backup = []
        
        for file_info in test_files:
            file_path = file_info['path']
            size = file_info['size']
            modified = file_info['modified']
            file_hash = file_info['hash']
            
            has_changed = tracker.has_file_changed(file_path, size, modified, file_hash)
            
            stored_info = tracker.get_file_info(file_path)
            
            print(f"📄 {file_path}")
            if stored_info:
                print(f"   Last backup: {stored_info.last_backup}")
                print(f"   Status: {'🔄 CHANGED - Will backup' if has_changed else '✅ No changes - Skip'}")
            else:
                print(f"   Status: 🆕 NEW FILE - Will backup")
            
            if has_changed:
                files_to_backup.append(file_info)
                tracker.update_file_info(file_path, size, modified, file_hash, "azure_blob")
            
            print()
        
        print(f"🚀 Fourth Backup: {len(files_to_backup)}/{len(test_files)} files uploaded")
        print(f"💡 Result: Only new file uploaded, existing files skipped!")
        print()
        
        # === SUMMARY ===
        print("=" * 60)
        print("📊 CHANGE DETECTION SUMMARY")
        print("=" * 60)
        
        stats = tracker.get_stats()
        print(f"📈 Tracking Statistics:")
        print(f"   • Total files tracked: {stats['total_files']}")
        print(f"   • Files with successful backups: {stats['backed_up_files']}")
        print(f"   • Total size tracked: {stats['total_size']:,} bytes")
        print()
        
        print("🔍 How Change Detection Works:")
        print("   1. 📊 SIZE CHECK - Compare file size with last backup")
        print("   2. 📅 TIMESTAMP CHECK - Compare last modified time")  
        print("   3. 🔒 HASH CHECK - Compare MD5 hash (optional)")
        print("   4. ✅ SKIP if no changes detected")
        print("   5. 🚀 UPLOAD if any change detected")
        print()
        
        print("💡 Benefits:")
        print("   • ⚡ Faster backups (skip unchanged files)")
        print("   • 💰 Lower costs (less data transfer)")
        print("   • 🌱 Reduced bandwidth usage")
        print("   • 📊 Detailed tracking and reporting")
        print()
        
        print("🔒 Detection Methods Available:")
        print("   • timestamp - Compare modification dates (fastest)")
        print("   • size - Compare file sizes (fast)")
        print("   • hash - Compare file content checksums (most accurate)")
        print("   • combined - Use all methods for maximum accuracy")
        
    finally:
        # Cleanup
        if tracker_file.exists():
            tracker_file.unlink()

def main():
    """Main function."""
    try:
        demo_change_detection()
        print("\n🎉 Change detection demo completed successfully!")
        return 0
        
    except KeyboardInterrupt:
        print("\n⚠️ Demo interrupted by user")
        return 1
    except Exception as e:
        print(f"\n💥 Demo failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
