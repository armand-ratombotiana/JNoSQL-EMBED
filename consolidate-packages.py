#!/usr/bin/env python3
"""
Comprehensive package consolidation script for JunifyDB.
Replaces all references to old package locations with new consolidated locations.
"""

import os
import re

# Define package mappings: old -> new
PACKAGE_MAPPINGS = {
    # Event package consolidation
    'org.junify.db.event.EventBus': 'org.junify.db.core.event.EventBus',
    
    # Metrics package consolidation
    'org.junify.db.metrics.DatabaseMetrics': 'org.junify.db.core.metrics.DatabaseMetrics',
    
    # Document package consolidation - use nosql.document as canonical
    'org.junify.db.document.Document': 'org.junify.db.nosql.document.Document',
    'org.junify.db.document.DocumentCollection': 'org.junify.db.nosql.document.DocumentCollection',
    'org.junify.db.document.Query': 'org.junify.db.nosql.document.Query',
    'org.junify.db.document.QueryCondition': 'org.junify.db.nosql.document.QueryCondition',
    'org.junify.db.document.AggregationPipeline': 'org.junify.db.nosql.document.AggregationPipeline',
    'org.junify.db.document.DocumentAggregation': 'org.junify.db.nosql.document.DocumentAggregation',
    'org.junify.db.document.TextSearch': 'org.junify.db.nosql.document.TextSearch',
    'org.junify.db.document.QueryExplain': 'org.junify.db.nosql.document.QueryExplain',
    'org.junify.db.document.VersionedDocument': 'org.junify.db.nosql.document.VersionedDocument',
    
    # KV package consolidation
    'org.junify.db.kv.KeyValueBucket': 'org.junify.db.nosql.kv.KeyValueBucket',
    
    # Column package consolidation
    'org.junify.db.column.ColumnFamily': 'org.junify.db.nosql.column.ColumnFamily',
    
    # Storage package consolidation - use storage.spi as canonical
    'org.junify.db.storage.StorageEngine': 'org.junify.db.storage.spi.StorageEngine',
    'org.junify.db.storage.FileEngine': 'org.junify.db.storage.spi.FileEngine',
    'org.junify.db.storage.InMemoryEngine': 'org.junify.db.storage.spi.InMemoryEngine',
    'org.junify.db.storage.BTreeEngine': 'org.junify.db.storage.spi.BTreeEngine',
    'org.junify.db.storage.LSMTreeEngine': 'org.junify.db.storage.spi.LSMTreeEngine',
    'org.junify.db.storage.H2StorageEngine': 'org.junify.db.storage.spi.H2StorageEngine',
    'org.junify.db.storage.WriteAheadLog': 'org.junify.db.storage.spi.WriteAheadLog',
    'org.junify.db.storage.FileEnginePool': 'org.junify.db.storage.spi.FileEnginePool',
    'org.junify.db.storage.DatabaseCompactor': 'org.junify.db.storage.spi.DatabaseCompactor',
    'org.junify.db.storage.BloomFilter': 'org.junify.db.storage.spi.BloomFilter',
    'org.junify.db.storage.DatabaseMetaManager': 'org.junify.db.storage.spi.DatabaseMetaManager',
    'org.junify.db.storage.QueryOptimizer': 'org.junify.db.storage.spi.QueryOptimizer',
    'org.junify.db.storage.SchemaManager': 'org.junify.db.storage.spi.SchemaManager',
    'org.junify.db.storage.FullTextSearchManager': 'org.junify.db.storage.spi.FullTextSearchManager',
    'org.junify.db.storage.ConstraintManager': 'org.junify.db.storage.spi.ConstraintManager',
    'org.junify.db.storage.TriggerManager': 'org.junify.db.storage.spi.TriggerManager',
    'org.junify.db.storage.ViewManager': 'org.junify.db.storage.spi.ViewManager',
    'org.junify.db.storage.SequenceManager': 'org.junify.db.storage.spi.SequenceManager',
    'org.junify.db.storage.StoredProcedureManager': 'org.junify.db.storage.spi.StoredProcedureManager',
    'org.junify.db.storage.UserManager': 'org.junify.db.storage.spi.UserManager',
    'org.junify.db.storage.ReplicationManager': 'org.junify.db.storage.spi.ReplicationManager',
    'org.junify.db.storage.CTEAndRecursiveManager': 'org.junify.db.storage.spi.CTEAndRecursiveManager',
    'org.junify.db.storage.WindowFunctionManager': 'org.junify.db.storage.spi.WindowFunctionManager',
    'org.junify.db.storage.AnalyticFunctionManager': 'org.junify.db.storage.spi.AnalyticFunctionManager',
    
    # Transaction package consolidation
    'org.junify.db.transaction.mvcc.Transaction': 'org.junify.db.transaction.Transaction',
    'org.junify.db.transaction.mvcc.MvccTransaction': 'org.junify.db.transaction.MvccTransaction',
    'org.junify.db.transaction.mvcc.MvccTransactionalCollection': 'org.junify.db.transaction.MvccTransactionalCollection',
    
    # Index package consolidation
    'org.junify.db.index.SecondaryIndex': 'org.junify.db.index.spi.SecondaryIndex',
    'org.junify.db.index.TextIndex': 'org.junify.db.index.spi.TextIndex',
}

# Files to skip (they define the old packages)
SKIP_FILES = {
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
}

def fix_file(filepath):
    """Apply package mappings to a file."""
    if filepath in SKIP_FILES:
        return False
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original = content
        
        # Apply all package mappings
        for old_pkg, new_pkg in PACKAGE_MAPPINGS.items():
            # Escape dots for regex
            old_escaped = old_pkg.replace('.', r'\.')
            content = re.sub(old_escaped, new_pkg, content)
        
        # Write back if changed
        if content != original:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
        return False
        
    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        return False

def main():
    """Process all Java files."""
    files_modified = 0
    files_skipped = 0
    files_processed = 0
    
    for root, dirs, files in os.walk('src/main/java'):
        for file in files:
            if not file.endswith('.java'):
                continue
            
            filepath = os.path.join(root, file)
            # Normalize path separators
            filepath = filepath.replace('\\', '/')
            
            files_processed += 1
            
            if filepath in SKIP_FILES:
                files_skipped += 1
                continue
            
            if fix_file(filepath):
                files_modified += 1
                print(f"Fixed: {filepath}")
    
    print(f"\n{'='*60}")
    print(f"Files processed: {files_processed}")
    print(f"Files skipped (package definitions): {files_skipped}")
    print(f"Files modified: {files_modified}")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
