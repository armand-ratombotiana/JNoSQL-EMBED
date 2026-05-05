#!/usr/bin/env python3
"""Fix SqlParser Token handling - add .value() to consumeIdentifier() calls"""

import re

with open('src/main/java/org/junify/db/sql/parser/SqlParser.java', 'r', encoding='utf-8') as f:
    lines = f.readlines()

fixed = 0
for i, line in enumerate(lines):
    # Only fix consumeIdentifier() that are used as values (assignments or arguments)
    # Don't fix method definitions (followed by {)
    if 'consumeIdentifier()' in line and '.value()' not in line:
        # Skip method definitions
        if 'private Token consumeIdentifier()' in line or 'public Token consumeIdentifier()' in line:
            continue
        # Fix variable assignments and method arguments
        new_line = line.replace('consumeIdentifier()', 'consumeIdentifier().value()')
        if new_line != line:
            lines[i] = new_line
            fixed += 1
            print(f"Fixed line {i+1}: {line.strip()[:50]}...")

with open('src/main/java/org/junify/db/sql/parser/SqlParser.java', 'w', encoding='utf-8') as f:
    f.writelines(lines)

print(f'\nFixed {fixed} lines')
