import secrets

def generate_secret_key(length=32):
    return secrets.token_hex(length // 2)

if __name__ == "__main__":
    secret_key = generate_secret_key()
    print(f"Generated Secret Key: {secret_key}")

# python generate_secret_key.py
