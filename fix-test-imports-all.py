#!/usr/bin/env python3
"""Fix all test file imports"""

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
        # Fix all incorrect package imports
        content = content.replace('org.junify.db.nosql.document.', 'org.junify.db.document.')
        content = content.replace('org.junify.db.nosql.kv.', 'org.junify.db.kv.')
        content = content.replace('org.junify.db.nosql.column.', 'org.junify.db.column.')
        content = content.replace('org.junify.db.console.http.', 'org.junify.db.console.')
        content = content.replace('org.junify.db.storage.SchemaManager.ColumnDef', 'org.junify.db.storage.SchemaManager')
        
        if content != original:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            fixed_count += 1
            print(f'Fixed: {filepath}')

print(f'\nTotal files fixed: {fixed_count}')
