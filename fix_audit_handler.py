file_path = r'C:\Users\jratombo-adm\Desktop\JNoSQL-EMBED\src\main\java\org\junify\db\console\http\JunifyDBServer.java'

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Add audit log handler to start() method
content = content.replace(
    'server.createContext("/api/constraints/", new ConstraintsHandler());\n\n        if (corsEnabled)',
    'server.createContext("/api/constraints/", new ConstraintsHandler());\n        server.createContext("/api/audit/logs", new AuditLogHandler());\n\n        if (corsEnabled)'
)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("Added audit log handler to start() method")
