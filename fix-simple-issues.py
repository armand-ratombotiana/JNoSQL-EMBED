#!/usr/bin/env python3
import re
import sys

def fix_file(filepath, replacements):
    """Apply regex replacements to a file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original = content
        for pattern, replacement in replacements.items():
            content = re.sub(pattern, replacement, content)
        
        if content != original:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Fixed: {filepath}")
            return True
        return False
    except Exception as e:
        print(f"Error processing {filepath}: {e}", file=sys.stderr)
        return False

def main():
    # Fix KeyValueBucket.java
    fix_file("src/main/java/org/junify/db/kv/KeyValueBucket.java", {
        r'import org\.junify\.db\.event\.EventBus': 'import org.junify.db.core.event.EventBus'
    })
    
    # Fix JunifyConsole.java
    fix_file("src/main/java/org/junify/db/console/JunifyConsole.java", {
        r'import org\.junify\.db\.transaction\.mvcc\.Transaction': 'import org.junify.db.transaction.Transaction'
    })
    
    # Fix SqlExecutor.java
    fix_file("src/main/java/org/junify/db/sql/executor/SqlExecutor.java", {
        r'java\.util\.ArrayList<org\.junify\.db\.nosql\.document\.Document>': 'java.util.List<org.junify.db.nosql.document.Document>'
    })
    
    # Fix SqlParser.java - Token to String conversions
    fix_file("src/main/java/org/junify/db/sql/parser/SqlParser.java", {
        r'var table = consumeIdentifier\(\);': 'var table = consumeIdentifier().value();',
        r'var col = consumeIdentifier\(\);': 'var col = consumeIdentifier().value();',
        r'col = consumeIdentifier\(\);': 'col = consumeIdentifier().value();'
    })
    
    print("Done!")

if __name__ == '__main__':
    main()
