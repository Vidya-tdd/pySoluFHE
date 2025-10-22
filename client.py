# pip install tenseal pycryptodome

import tenseal as ts
import sqlite3
import pickle
import socket
import json
from typing import List, Tuple

# ==================== DATABASE SETUP ====================

def setup_database():
    """Create and populate SQLite database with parties data"""
    conn = sqlite3.connect('parties.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS parties (
        party_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT,
        address TEXT,
        date_of_birth TEXT,
        region TEXT
        )
        ''')

    # Sample data
    sample_data = [
        (1005,'Stace Roy','stacroy@gmail.com','12 Block2 New York,NY-876RD','12-FEB-1985','US'),
        (1006,'Liam Neeson','liamNee@gmail.com','45 Block7 Los Angeles,CA-345TY','07-JUL-1978','US')
    ]

    cursor.executemany('INSERT OR REPLACE INTO accounts VALUES (?, ?, ?, ?, ?, ?)', sample_data)
    conn.commit()
    conn.close()
    print("Database setup complete!")

# Convert dataframe into a list of tuples for database insertion
def df_to_tuples(df) -> List[Tuple]:
    """Convert DataFrame to list of tuples for database insertion"""
    records = []
    for _, row in df.iterrows():
        records.append((
            int(row['PartyId']),
            row['Name'],
            row['Email'],
            row['Address'],
            row['DateOfBirth'],
            row['Region']
        ))
    return records   

#Insert the tuples into the database
def insert_parties_to_db(tuples_list: List[Tuple]):
    """Insert list of party tuples into the database"""
    conn = sqlite3.connect('parties.db')
    cursor = conn.cursor()
    cursor.executemany('INSERT OR REPLACE INTO parties VALUES (?, ?, ?, ?, ?, ?)', tuples_list)
    conn.commit()
    conn.close()
    print(f"Inserted {len(tuples_list)} party records into the database.")

# ==================== CLIENT SIDE ====================

class FHEClient:
    def __init__(self):
        # Create TenSEAL context for FHE operations
        self.context = ts.context(
            ts.SCHEME_TYPE.CKKS,
            poly_modulus_degree=8192,
            coeff_mod_bit_sizes=[60, 40, 40, 60]
        )
        self.context.generate_galois_keys()
        self.context.global_scale = 2**40

    def fetch_from_db(self, party_id: int) -> Tuple[int, str]:
        """Fetch party id and name from SQLite database"""
        conn = sqlite3.connect('parties.db')
        cursor = conn.cursor()

        cursor.execute('SELECT party_id, name FROM parties WHERE party_id = ?',
                       (party_id,))
        result = cursor.fetchone()
        conn.close()

        if result:
            return result
        return None, None

    def encrypt_data(self, party_id: int, name: str) -> dict:
        """Encrypt party id and name using FHE"""
        # Encrypt party id (as a vector)
        encrypted_ptyid = ts.ckks_vector(self.context, [float(party_id)])

        # Encrypt name (convert to ASCII values)
        name_ascii = [float(ord(c)) for c in name]
        encrypted_name = ts.ckks_vector(self.context, name_ascii)

        return {
            'encrypted_partyid': encrypted_ptyid.serialize(),
            'encrypted_name': encrypted_name.serialize(),
            'name_length': len(name)
        }

    def send_to_server(self, encrypted_data: dict, host='localhost', port=9999):
        """Send encrypted data to server"""
        # Also send public context (without secret key)
        public_context = self.context.serialize(save_secret_key=False)

        payload = {
            'context': public_context,
            'data': encrypted_data
        }

        # Serialize and send
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((host, port))

        serialized = pickle.dumps(payload)
        client_socket.sendall(len(serialized).to_bytes(4, 'big'))
        client_socket.sendall(serialized)

        # Receive encrypted results
        result_size_data = client_socket.recv(4)
        if not result_size_data:
            client_socket.close()
            raise ConnectionError("No data received from server")
        result_size = int.from_bytes(result_size_data, 'big')
        result_data = b''
        while len(result_data) < result_size:
            chunk = client_socket.recv(4096)
            if not chunk:
                break
            result_data += chunk

        client_socket.close()
        return pickle.loads(result_data)

    def decrypt_results(self, encrypted_results: dict) -> dict:
        """Decrypt results received from server"""
        results = {}

        # Decrypt account number
        enc_acc = ts.ckks_vector_from(self.context, encrypted_results['encrypted_account'])
        results['account_number'] = int(enc_acc.decrypt()[0])

        # Decrypt name
        enc_name = ts.ckks_vector_from(self.context, encrypted_results['encrypted_name'])
        name_ascii = enc_name.decrypt()
        results['name'] = ''.join([chr(int(round(val))) for val in name_ascii])

        # Decrypt balance
        enc_balance = ts.ckks_vector_from(self.context, encrypted_results['encrypted_balance'])
        results['balance'] = enc_balance.decrypt()[0]

        # Decrypt email
        enc_email = ts.ckks_vector_from(self.context, encrypted_results['encrypted_email'])
        email_ascii = enc_email.decrypt()
        results['email'] = ''.join([chr(int(round(val))) for val in email_ascii])

        return results