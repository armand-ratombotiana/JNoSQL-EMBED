#!/usr/bin/env python3
"""
Comprehensive fix script for ALL remaining JunifyDB compilation errors.
This script fixes package consolidation, type mismatches, and API issues.
"""

import os
import re

def fix_all_files():
    """Apply all fixes to all Java files."""
    
    # Define all package consolidations
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
        
        # Index package - fix spi reference
        r'\borg\.junify\.db\.index\.spi\.SecondaryIndex\b': 'org.junify.db.index.SecondaryIndex',
        r'\borg\.junify\.db\.index\.spi\.TextIndex\b': 'org.junify.db.index.TextIndex',
    }
    
    # Files to skip (they define the old packages)
    skip_files = {
        'src/main/java/org/junify/db/event/EventBus.java',
        'src/main/java/org/junify/db/metrics/DatabaseMetrics.java',
        'src/main/java/org/junify/db/document/Document.java',
        'src/main/java/org/junify/db/document/DocumentCollection.java',
        'src/main/java/org/junify/db/kv/KeyValueBucket.java',
        'src/main/java/org/junify/db/column/ColumnFamily.java',
        'src/main/java/org/junify/db/storage/StorageEngine.java',
        'src/main/java/org/junify/db/storage/FileEngine.java',
        'src/main/java/org/junify/db/storage/InMemoryEngine.java',
        'src/main/java/org/junify/db/storage/BTreeEngine.java',
        'src/main/java/org/junify/db/storage/LSMTreeEngine.java',
        'src/main/java/org/junify/db/storage/H2StorageEngine.java',
        'src/main/java/org/junify/db/transaction/mvcc/Transaction.java',
        'src/main/java/org/junify/db/index/SecondaryIndex.java',
    }
    
    files_modified = 0
    files_skipped = 0
    
    for root, dirs, files in os.walk('src/main/java'):
        for file in files:
            if not file.endswith('.java'):
                continue
            
            filepath = os.path.join(root, file)
            filepath_normalized = filepath.replace('\\', '/')
            
            if filepath_normalized in skip_files:
                files_skipped += 1
                continue
            
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                original = content
                
                # Apply all package mappings
                for pattern, replacement in package_mappings.items():
                    content = re.sub(pattern, replacement, content)
                
                # Fix ThreadPerTaskExecutor (doesn't exist in Java 25)
                if 'ThreadPerTaskExecutor' in content:
                    content = content.replace(
                        'import java.util.concurrent.ThreadPerTaskExecutor;',
                        'import java.util.concurrent.Executors;'
                    )
                    content = content.replace(
                        'new ThreadPerTaskExecutor()',
                        'Executors.newVirtualThreadPerTaskExecutor()'
                    )
                
                # Fix SqlParser Token to String conversions
                if 'SqlParser.java' in file:
                    content = re.sub(
                        r'var table = consumeIdentifier\(\);',
                        'var table = consumeIdentifier().value();',
                        content
                    )
                    content = re.sub(
                        r'var col = consumeIdentifier\(\);',
                        'var col = consumeIdentifier().value();',
                        content
                    )
                    content = re.sub(
                        r'(?<![.\w])col = consumeIdentifier\(\);',
                        'col = consumeIdentifier().value();',
                        content
                    )
                
                # Fix SqlExecutor ArrayList to List
                if 'SqlExecutor.java' in file:
                    content = content.replace(
                        'ArrayList<org.junify.db.nosql.document.Document>',
                        'List<org.junify.db.nosql.document.Document>'
                    )
                    # Add List import if needed
                    if 'List<' in content and 'import java.util.List;' not in content:
                        content = content.replace(
                            'import java.util.*;',
                            'import java.util.*;\nimport java.util.List;'
                        )
                
                # Fix UnifiedRecord metadata issue
                if 'UnifiedRecord.java' in file:
                    content = content.replace(
                        'metadata().nextVersion()',
                        'metadata().version() + 1'
                    )
                    content = content.replace(
                        'metadata().withVersion(version)',
                        'metadata()'
                    )
                
                # Write back if changed
                if content != original:
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified += 1
                    print(f"Fixed: {filepath}")
                    
            except Exception as e:
                print(f"Error processing {filepath}: {e}")
    
    print(f"\n{'='*60}")
    print(f"Files skipped (package definitions): {files_skipped}")
    print(f"Files modified: {files_modified}")
    print(f"{'='*60}")

if __name__ == '__main__':
    fix_all_files()
