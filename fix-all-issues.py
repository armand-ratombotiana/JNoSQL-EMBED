#!/usr/bin/env python3
"""
Comprehensive fix script for JunifyDB compilation errors.
Makes all necessary import and type fixes in a single pass.
"""

import os
import re

def fix_all_files():
    """Apply all fixes to all Java files."""
    
    # Define all replacements
    replacements = {
        # Fix EventBus imports
        r'import org\.junify\.db\.event\.EventBus': 'import org.junify.db.core.event.EventBus',
        
        # Fix Transaction imports  
        r'import org\.junify\.db\.transaction\.mvcc\.Transaction': 'import org.junify.db.transaction.Transaction',
        
        # Fix Document imports
        r'import org\.junify\.db\.document\.Document;': 'import org.junify.db.nosql.document.Document;',
        
        # Fix ArrayList to List
        r'ArrayList<org\.junify\.db\.nosql\.document\.Document>': 'List<org.junify.db.nosql.document.Document>',
        
        # Fix DatabaseMetrics imports
        r'import org\.junify\.db\.metrics\.DatabaseMetrics': 'import org.junify.db.core.metrics.DatabaseMetrics',
    }
    
    # Files that need SqlParser Token fixes
    sql_parser_fixes = {
        r'var table = consumeIdentifier\(\);': 'var table = consumeIdentifier().value();',
        r'var col = consumeIdentifier\(\);': 'var col = consumeIdentifier().value();',
        r'(?<![.\w])col = consumeIdentifier\(\);': 'col = consumeIdentifier().value();',
    }
    
    files_modified = 0
    
    # Walk through all Java files
    for root, dirs, files in os.walk('src/main/java'):
        for file in files:
            if not file.endswith('.java'):
                continue
                
            filepath = os.path.join(root, file)
            
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                original = content
                
                # Apply general replacements
                for pattern, replacement in replacements.items():
                    content = re.sub(pattern, replacement, content)
                
                # Apply SqlParser-specific fixes
                if 'SqlParser.java' in file:
                    for pattern, replacement in sql_parser_fixes.items():
                        content = re.sub(pattern, replacement, content)
                
                # Write back if changed
                if content != original:
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(content)
                    print(f"Fixed: {filepath}")
                    files_modified += 1
                    
            except Exception as e:
                print(f"Error processing {filepath}: {e}")
    
    print(f"\nTotal files modified: {files_modified}")

if __name__ == '__main__':
    fix_all_files()
