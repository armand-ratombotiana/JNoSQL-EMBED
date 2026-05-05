#!/usr/bin/env python3
"""
Phase 1: Fix all test file import statements.
Updates all package references to use consolidated package names.
"""

import os
import re

def fix_test_files():
    """Apply package consolidation to all test files."""
    
    # Define all package mappings
    package_mappings = {
        # Event package
        r'\borg\.junify\.db\.event\.EventBus\b': 'org.junify.db.core.event.EventBus',
        
        # Metrics package
        r'\borg\.junify\.db\.metrics\.DatabaseMetrics\b': 'org.junify.db.core.metrics.DatabaseMetrics',
        
        # Storage package - use spi as canonical
        r'\borg\.junify\.db\.storage\.StorageEngine\b': 'org.junify.db.storage.spi.StorageEngine',
        r'\borg\.junify\.db\.storage\.FileEngine\b': 'org.junify.db.storage.spi.FileEngine',
        r'\borg\.junify\.db\.storage\.InMemoryEngine\b': 'org.junify.db.storage.spi.InMemoryEngine',
        r'\borg\.junify\.db\.storage\.BTreeEngine\b': 'org.junify.db.storage.spi.BTreeEngine',
        r'\borg\.junify\.db\.storage\.LSMTreeEngine\b': 'org.junify.db.storage.spi.LSMTreeEngine',
        r'\borg\.junify\.db\.storage\.H2StorageEngine\b': 'org.junify.db.storage.spi.H2StorageEngine',
        r'\borg\.junify\.db\.storage\.WriteAheadLog\b': 'org.junify.db.storage.spi.WriteAheadLog',
        r'\borg\.junify\.db\.storage\.FileEnginePool\b': 'org.junify.db.storage.spi.FileEnginePool',
        r'\borg\.junify\.db\.storage\.DatabaseCompactor\b': 'org.junify.db.storage.spi.DatabaseCompactor',
        
        # Document package - use nosql.document as canonical
        r'\borg\.junify\.db\.document\.Document\b': 'org.junify.db.nosql.document.Document',
        r'\borg\.junify\.db\.document\.DocumentCollection\b': 'org.junify.db.nosql.document.DocumentCollection',
        r'\borg\.junify\.db\.document\.Query\b': 'org.junify.db.nosql.document.Query',
        r'\borg\.junify\.db\.document\.QueryCondition\b': 'org.junify.db.nosql.document.QueryCondition',
        r'\borg\.junify\.db\.document\.AggregationPipeline\b': 'org.junify.db.nosql.document.AggregationPipeline',
        r'\borg\.junify\.db\.document\.DocumentAggregation\b': 'org.junify.db.nosql.document.DocumentAggregation',
        r'\borg\.junify\.db\.document\.TextSearch\b': 'org.junify.db.nosql.document.TextSearch',
        r'\borg\.junify\.db\.document\.QueryExplain\b': 'org.junify.db.nosql.document.QueryExplain',
        r'\borg\.junify\.db\.document\.VersionedDocument\b': 'org.junify.db.nosql.document.VersionedDocument',
        
        # KV package
        r'\borg\.junify\.db\.kv\.KeyValueBucket\b': 'org.junify.db.nosql.kv.KeyValueBucket',
        
        # Column package
        r'\borg\.junify\.db\.column\.ColumnFamily\b': 'org.junify.db.nosql.column.ColumnFamily',
        
        # Transaction package
        r'\borg\.junify\.db\.transaction\.mvcc\.Transaction\b': 'org.junify.db.transaction.Transaction',
        r'\borg\.junify\.db\.transaction\.mvcc\.MvccTransaction\b': 'org.junify.db.transaction.MvccTransaction',
        r'\borg\.junify\.db\.transaction\.mvcc\.MvccTransactionalCollection\b': 'org.junify.db.transaction.MvccTransactionalCollection',
        
        # Index package
        r'\borg\.junify\.db\.index\.spi\.SecondaryIndex\b': 'org.junify.db.index.SecondaryIndex',
        r'\borg\.junify\.db\.index\.spi\.TextIndex\b': 'org.junify.db.index.TextIndex',
        
        # JunifyDB main class
        r'\borg\.junify\.db\.Junify\b': 'org.junify.db.JunifyDB',
    }
    
    files_modified = 0
    
    # Walk through all test files
    for root, dirs, files in os.walk('src/test/java'):
        for file in files:
            if not file.endswith('.java'):
                continue
            
            filepath = os.path.join(root, file)
            
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                original = content
                
                # Apply all package mappings
                for pattern, replacement in package_mappings.items():
                    content = re.sub(pattern, replacement, content)
                
                # Write back if changed
                if content != original:
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified += 1
                    print(f"Fixed: {filepath}")
                    
            except Exception as e:
                print(f"Error processing {filepath}: {e}")
    
    print(f"\n{'='*60}")
    print(f"Phase 1 Complete: Fixed {files_modified} test files")
    print(f"{'='*60}")

if __name__ == '__main__':
    fix_test_files()
