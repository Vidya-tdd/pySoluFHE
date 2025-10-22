# ==================== SERVER SIDE ====================

class FHEServer:

    def __init__(self):
        self.context = None


    def load_context(self, serialized_context):
        """Load public context from client"""
        self.context = ts.context_from(serialized_context)

    # Step 1: Create TenSEAL contexts for both CKKS and BFV schemes
    def create_contexts():
        """Create and configure TenSEAL contexts for CKKS (floats) and BFV (integers/strings)"""
    # CKKS context for floating point numbers
        ckks_context = ts.context(
        ts.SCHEME_TYPE.CKKS,
        poly_modulus_degree=8192,
        coeff_mod_bit_sizes=[60, 40, 40, 60]
        )
        ckks_context.global_scale = 2**40
        ckks_context.generate_galois_keys()

        # BFV context for integers (used for string encoding)
        bfv_context = ts.context(
        ts.SCHEME_TYPE.BFV,
        poly_modulus_degree=4096,
        plain_modulus=1032193
        )
        bfv_context.generate_galois_keys()

        return ckks_context, bfv_context

    def search_encrypted(self, encrypted_data: dict) -> dict:
        """Perform search on encrypted data and return encrypted results"""
        # Deserialize encrypted party id
        enc_partyid = ts.ckks_vector_from(self.context, encrypted_data['encrypted_partyid'])

# Decrypt on server side to search (in real FHE, you'd compare encrypted values)
# In practice, server would work on encrypted data without decryption
# Here we simulate finding the match
        conn = sqlite3.connect('accounts.db')
        cursor = conn.cursor()

# For demo: we'll search based on encrypted search pattern
# In real FHE, comparison happens on encrypted data
        cursor.execute('SELECT * FROM accounts where party_id = ?')
        results = cursor.fetchall()
        conn.close()

# Find matching record (simplified - in real FHE this would be encrypted comparison)
# We'll return the first match encrypted
        if results:
            result = results[0] # Simplified: return first record

# Encrypt all fields for return
        encrypted_result = {
        'encrypted_account': ts.ckks_vector(self.context, [float(result[0])]).serialize(),
        'encrypted_name': ts.ckks_vector(self.context,
        [float(ord(c)) for c in result[1]]).serialize(),
        'encrypted_balance': ts.ckks_vector(self.context, [float(result[2])]).serialize(),
        'encrypted_email': ts.ckks_vector(self.context,
        [float(ord(c)) for c in result[3]]).serialize()
        }

        return encrypted_result

return None

def start_server(self, host='localhost', port=9999):
    """Start FHE server"""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(1)
    print(f"FHE Server listening on {host}:{port}")

    while True:
        client_socket, address = server_socket.accept()
        print(f"Connection from {address}")

    # Receive data
    data_size = int.from_bytes(client_socket.recv(4), 'big')
    data = b''
    while len(data) < data_size:
        data += client_socket.recv(4096)

    payload = pickle.loads(data)

    # Load context and process
    self.load_context(payload['context'])
    encrypted_results = self.search_encrypted(payload['data'])

    # Send encrypted results back
    result_data = pickle.dumps(encrypted_results)
    client_socket.sendall(len(result_data).to_bytes(4, 'big'))
    client_socket.sendall(result_data)

    client_socket.close()
    print("Request processed")

#Set up the database for accounts and payments
    
def setup_database():
    """Create and populate SQLite database with accounts data"""
    conn = sqlite3.connect('accounts.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS accounts (
        account_id TEXT PRIMARY KEY,
        party_id INTEGER FOREIGN KEY,
        region TEXT,
        account_type TEXT
        )
        ''')

    """Create and populate SQLite database with payments data"""
    conn = sqlite3.connect('payments.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS payments (
        payment_id TEXT PRIMARY KEY,
        fromAccountId TEXT FOREIGN KEY,
        toAccountId TEXT FOREIGN KEY,
        amount REAL,
        currency TEXT,
        paymentDate TEXT,
        paymentType TEXT
        )
        ''')

    # Sample data
    sample_data_accounts = [
        ('A0000004',1004,'US','Savings'),
        ('A0000005',1005,'EU','Checking')
    ]

    sample_data_payments = [
        ('P0000005','A0000004','A0000005',500.0,'USD','2023-09-15','Transfer'),
        ('P0000006','A0000005','A0000004',300.0,'EUR','2023-09-16','Payment')   
    ]
    cursor.executemany('INSERT OR REPLACE INTO accounts VALUES (?, ?, ?, ?)', sample_data_accounts)
    cursor.executemany('INSERT OR REPLACE INTO payments VALUES (?, ?, ?, ?, ?, ?, ?)', sample_data_payments)
    conn.commit()
    conn.close()
    print("Database setup complete!")


#Encrypt the dataframe columns and insert into DB
# Step 2: Read CSV file
#Read accounts.csv to fetch AccountId, PartyId, Region, AccountType
def load_accounts_csv_to_db(file_path: str):
    """Load CSV file and insert into database"""
    df = pd.read_csv("./accounts.csv")
    print(f"Loaded CSV with {len(df)} rows and {len(df.columns)} columns")
    print(f"Columns: {df.columns.tolist()}")
    #Pass the dataframe to the server to insert into DB
    df_to_tuples_list = df_accounts_to_tuples(df)
    insert_accounts_to_db(df_to_tuples_list)   
    encrypted_data = encrypt_dataframe(df) 
    save_to_database(encrypted_data, ckks_context, bfv_context,'accounts_encrypted.db')
    return df


# Convert accounts dataframe into a list of tuples for database insertion
def df_accounts_to_tuples(df) -> List[Tuple]:
    """Convert DataFrame to list of tuples for database insertion"""
    records = []
    for _, row in df.iterrows():
        records.append((
            int(row['Account']),
            row['PartyId'],
            row['Region'],
            row['AccountType']
        ))
    return records   

#Insert the accounts tuples into the database
def insert_accounts_to_db(tuples_list: List[Tuple]):
    """Insert list of accounts tuples into the database"""
    conn = sqlite3.connect('accounts.db')
    cursor = conn.cursor()
    cursor.executemany('INSERT OR REPLACE INTO accounts VALUES (?, ?, ?, ?, ?, ?)', tuples_list)
    conn.commit()
    conn.close()
    print(f"Inserted {len(tuples_list)} accounts records into the database.")


#Read payments.csv to fetch PaymentId, FromAccountId, ToAccountId, Amount, Currency, PaymentDate, PaymentType
def load_payments_csv_to_db(file_path: str):
    """Load CSV file and insert into database"""
    df = pd.read_csv("./payments.csv")
    print(f"Loaded CSV with {len(df)} rows and {len(df.columns)} columns")
    print(f"Columns: {df.columns.tolist()}")
    #Pass the dataframe to the server to insert into DB
    df_to_tuples_list = df_payments_to_tuples(df)
    insert_payments_to_db(df_to_tuples_list)   
    encrypted_data = encrypt_dataframe(df)
    save_to_database(encrypted_data, ckks_context, bfv_context,'payments_encrypted.db')
    return df

# Convert payments dataframe into a list of tuples for database insertion
def df_payments_to_tuples(df) -> List[Tuple]:
    """Convert DataFrame to list of tuples for database insertion"""
    records = []
    for _, row in df.iterrows():
        records.append((
            int(row['PaymentId']),
            row['FromAccountId'],
            row['ToAccountId'],
            row['Amount'],
            row['Currency'],
            row['PaymentDate'],
            row['PaymentType']
        ))
    return records   

#Insert the payments tuples into the database
def insert_payments_to_db(tuples_list: List[Tuple]):
    """Insert list of payments tuples into the database"""
    conn = sqlite3.connect('payments.db')
    cursor = conn.cursor()
    cursor.executemany('INSERT OR REPLACE INTO payments VALUES (?, ?, ?, ?, ?, ?, ?)', tuples_list)
    conn.commit()
    conn.close()
    print(f"Inserted {len(tuples_list)} payments records into the database.")


# Helper function to encode strings to integers
def string_to_integers(text, max_length=100):
    """Convert string to list of integers (ASCII/Unicode values)"""
# Convert each character to its Unicode code point
    integers = [ord(char) for char in text[:max_length]]
    # Pad with zeros if needed
    while len(integers) < max_length:
        integers.append(0)
    return integers

def integers_to_string(integers):
    """Convert list of integers back to string"""
# Convert integers back to characters, stop at first null (0)
    chars = []
    for num in integers:
        if num == 0:
            break
        chars.append(chr(int(num)))
        return ''.join(chars)

# Step 3: Encrypt data using CKKS for numbers and BFV for strings
def encrypt_dataframe(df, ckks_context, bfv_context):
    """Encrypt numerical columns with CKKS and string columns with BFV"""
    encrypted_data = []

    for idx, row in df.iterrows():
        encrypted_row = {}

    for col in df.columns:
        value = row[col]

# Handle numerical data with CKKS
    if pd.api.types.is_numeric_dtype(df[col]) and pd.notna(value):
        encrypted_value = ts.ckks_vector(ckks_context, [float(value)])
        encrypted_row[col] = {
        'data': encrypted_value.serialize(),
        'scheme': 'CKKS'
        }
    # Handle string data with BFV
    elif pd.notna(value):
    # Convert string to integers
        int_values = string_to_integers(str(value))
    # Encrypt with BFV
        encrypted_value = ts.bfv_vector(bfv_context, int_values)
        encrypted_row[col] = {
        'data': encrypted_value.serialize(),
        'scheme': 'BFV'
        }
    else:
    # Handle NaN/None values
        encrypted_row[col] = {
        'data': b'',
        'scheme': 'NULL'
        }

    encrypted_row['row_id'] = idx
    encrypted_data.append(encrypted_row)

    return encrypted_data

# Step 4: Save to database
def save_to_database(encrypted_data, ckks_context, bfv_context, db_name):
    """Save encrypted data and contexts to SQLite database"""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create table for encrypted data with scheme information
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS encrypted_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    row_id INTEGER,
    column_name TEXT,
    encrypted_value BLOB,
    scheme TEXT
    )
    ''')

    # Create table for contexts
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS encryption_context (
    id INTEGER PRIMARY KEY,
    context_type TEXT,
    context_data BLOB
    )
    ''')

    # Insert encrypted data
    for record in encrypted_data:
        row_id = record['row_id']
    for col, value in record.items():
        if col != 'row_id':
            cursor.execute(
        'INSERT INTO encrypted_records (row_id, column_name, encrypted_value, scheme) VALUES (?, ?, ?, ?)',
        (row_id, col, value['data'], value['scheme'])
        )

    # Save contexts
    cursor.execute('DELETE FROM encryption_context') # Clear old contexts
    cursor.execute('INSERT INTO encryption_context (id, context_type, context_data) VALUES (?, ?, ?)',
    (1, 'CKKS', ckks_context.serialize()))
    cursor.execute('INSERT INTO encryption_context (id, context_type, context_data) VALUES (?, ?, ?)',
    (2, 'BFV', bfv_context.serialize()))

    conn.commit()
    conn.close()
    print(f"Data successfully encrypted and saved to {db_name}")
    print(f" - CKKS scheme used for numerical data")
    print(f" - BFV scheme used for string data")

 