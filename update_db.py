import sqlite3
import os

db_path = os.path.join(os.getcwd(), 'app.db')
print(f"Connecting to database at: {db_path}")

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 1. Create User table
print("Creating 'user' table...")
cursor.execute('''
    CREATE TABLE IF NOT EXISTS user (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username VARCHAR(64) NOT NULL UNIQUE,
        email VARCHAR(120) UNIQUE,
        password_hash VARCHAR(256)
    )
''')

# 2. Add user_id column to other tables
tables_to_update = [
    'investment_platform',
    'transaction',
    'account',
    'category',
    'debt',
    'banking_transaction',
    'recurring_payment'
]

for table in tables_to_update:
    print(f"Checking table '{table}' for 'user_id' column...")
    cursor.execute(f'PRAGMA table_info("{table}")')
    columns = [info[1] for info in cursor.fetchall()]
    if 'user_id' not in columns:
        print(f"Adding 'user_id' to '{table}'...")
        try:
            cursor.execute(f'ALTER TABLE "{table}" ADD COLUMN user_id INTEGER REFERENCES user(id)')
            # Set default user_id = 1 for existing records (assuming single user migration)
            cursor.execute(f'UPDATE "{table}" SET user_id = 1')
        except Exception as e:
             print(f"Error adding column to {table}: {e}")
    else:
        print(f"'user_id' already exists in '{table}'.")

# 3. Create a default user if not exists
print("Checking for default user...")
cursor.execute("SELECT id FROM user WHERE id = 1")
if not cursor.fetchone():
    print("Creating default user 'admin' with password 'admin'...")
    # Hash for 'admin' generated using werkzeug.security.generate_password_hash('admin')
    # scrypt:32768:8:1$wW... (I'll use a simple placeholder and rely on the app to set it properly if needed, 
    # or generate a real one here. Let's generate a real one using a separate small script or just inserting a known hash)
    # Actually, I can import werkzeug here.
    from werkzeug.security import generate_password_hash
    p_hash = generate_password_hash('admin')
    cursor.execute("INSERT INTO user (id, username, password_hash) VALUES (1, 'admin', ?)", (p_hash,))

conn.commit()
conn.close()
print("Database update complete.")
