# ==================== IMPORTS ====================
import sqlite3
import tenseal as ts
import socket   
import pandas as pd  
from client import FHEClient, setup_database
from server import FHEServer
# ==================== MAIN EXECUTION ====================

def run_client_example():
    """Run client-side example"""
    print("\n=== CLIENT SIDE ===")

# Initialize client
    client = FHEClient()

#Read parties.csv to fetch PartyId, Name
# Step 2: Read CSV file
def read_csv(file_path):
    """Read CSV file and return DataFrame"""
    df = pd.read_csv("./parties.csv")
    print(f"Loaded CSV with {len(df)} rows and {len(df.columns)} columns")
    print(f"Columns: {df.columns.tolist()}")
    return df

# Fetch data from database
    account_num = 1001
    acc_number, name = client.fetch_from_db(account_num)
    print(f"Fetched from DB: Account={acc_number}, Name={name}")

# Encrypt data
print("Encrypting data using FHE...")
encrypted_data = client.encrypt_data(acc_number, name)
print("Data encrypted successfully!")

# Send to server and receive encrypted results
print("Sending encrypted data to server...")
encrypted_results = client.send_to_server(encrypted_data)

# Decrypt results
print("Decrypting results...")
decrypted_results = client.decrypt_results(encrypted_results)

print("\n=== DECRYPTED RESULTS ===")
print(f"Account Number: {decrypted_results['account_number']}")
print(f"Name: {decrypted_results['name']}")
print(f"Balance: ${decrypted_results['balance']:.2f}")
print(f"Email: {decrypted_results['email']}")

def run_server():
    """Run server-side"""
    print("\n=== SERVER SIDE ===")
    server = FHEServer()
    server.start_server()

    if __name__ == "__main__":
        import sys

# Setup database first
    setup_database()

if len(sys.argv) > 1 and sys.argv[1] == 'server':
# Run as server
    run_server()
else:
# Run as client
    print("Starting client (run with 'python script.py server' to start server)")
    print("Make sure server is running first!")
    import time
    time.sleep(2)
    run_client_example()