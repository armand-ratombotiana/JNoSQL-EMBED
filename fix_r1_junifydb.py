import re

file_path = r'C:\Users\jratombo-adm\Desktop\JNoSQL-EMBED\src\main\java\org\junify\db\JunifyDB.java'

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Add SSL variables in main method
content = content.replace(
    'String apiKey = null;',
    '''String apiKey = null;
        int sslPort = -1;
        String sslKeystorePath = null;
        String sslKeystorePassword = null;'''
)

# 2. Add SSL CLI flags in the switch statement
content = content.replace(
    '''case "--api-key" -> apiKey = args[++i];
                case "--help" ->''',
    '''case "--api-key" -> apiKey = args[++i];
                case "--ssl-port" -> sslPort = Integer.parseInt(args[++i]);
                case "--ssl-keystore" -> sslKeystorePath = args[++i];
                case "--ssl-keypass" -> sslKeystorePassword = args[++i];
                case "--help" ->'''
)

# 3. Update help message
help_text = '''System.out.println("  --api-key <key>        API key for authentication (optional)");
                    System.out.println("  --ssl-port <port>      SSL/TLS port (optional, requires --ssl-keystore)");
                    System.out.println("  --ssl-keystore <path>  Path to JKS keystore file (optional)");
                    System.out.println("  --ssl-keypass <pass>   Keystore password (optional)");
                    System.out.println("  --help                 Show this help");'''

content = content.replace(
    '''System.out.println("  --api-key <key>        API key for authentication (optional)");
                    System.out.println("  --help                 Show this help");''',
    help_text
)

# 4. Add SSL configuration after server start
ssl_startup = '''if (apiKey != null && !apiKey.isEmpty()) {
                server.setApiKey(apiKey);
                System.out.println("API authentication enabled");
            }
            // Configure SSL if specified
            if (sslPort > 0 && sslKeystorePath != null) {
                server.configureSsl(sslPort, sslKeystorePath, sslKeystorePassword != null ? sslKeystorePassword : "");
                System.out.println("SSL/TLS enabled on port " + sslPort);
            }'''

content = content.replace(
    '''if (apiKey != null && !apiKey.isEmpty()) {
                server.setApiKey(apiKey);
                System.out.println("API authentication enabled");
            }''',
    ssl_startup
)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("R1 SSL CLI flags added to JunifyDB.java!")
