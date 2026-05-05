#!/usr/bin/env python3
"""Fix all test files to use correct class names"""

import os

test_dir = 'src/test/java'
fixed_count = 0

for root, dirs, files in os.walk(test_dir):
    for file in files:
        if not file.endswith('.java'):
            continue
        
        filepath = os.path.join(root, file)
        with open(filepath, 'r', encoding='utf-8') as f:
            original = f.read()
        
        content = original
        # Fix class names
        content = content.replace('private Junify db;', 'private JunifyDB db;')
        content = content.replace('Junify.embed()', 'JunifyDB.embed()')
        content = content.replace('import org.junify.db.Junify;', 'import org.junify.db.JunifyDB;')
        
        if content != original:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            fixed_count += 1
            print(f'Fixed: {filepath}')

print(f'\nTotal files fixed: {fixed_count}')
