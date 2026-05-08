import re

content = open('c:/Users/jratombo-adm/Desktop/JNoSQL-EMBED/src/main/java/org/junify/db/storage/spi/UserManager.java', 'r', encoding='utf-8').read()

# Fix SQL injection - replace string concatenation with parameterized query
old_pattern = r"SELECT password_hash, salt FROM db_users WHERE username = '\s*\+\s*username\s*\+\s*' AND enabled = TRUE"
new_code = 'SELECT password_hash, salt FROM db_users WHERE username = ? AND enabled = TRUE",\n            username'

# Simple replacement
content = content.replace(
    "SELECT password_hash, salt FROM db_users WHERE username = '\" + username + \"' AND enabled = TRUE",
    'SELECT password_hash, salt FROM db_users WHERE username = ? AND enabled = TRUE",\n            username'
)

open('c:/Users/jratombo-adm/Desktop/JNoSQL-EMBED/src/main/java/org/junify/db/storage/spi/UserManager.java', 'w', encoding='utf-8').write(content)
print('Fixed SQL injection in UserManager.java')
