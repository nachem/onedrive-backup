"""Encryption utilities for client-side file encryption."""

import os
import base64
from typing import Optional, Union
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

class EncryptionHelper:
    """Helper class for file encryption and decryption."""
    
    def __init__(self, encryption_key: Optional[str] = None):
        """Initialize encryption helper.
        
        Args:
            encryption_key: Base64 encoded encryption key, or None to disable encryption
        """
        self.encryption_key = encryption_key
        self._fernet = None
        
        if encryption_key:
            try:
                self._fernet = Fernet(encryption_key.encode())
            except Exception:
                raise ValueError("Invalid encryption key provided")
    
    @classmethod
    def generate_key(cls) -> str:
        """Generate a new encryption key.
        
        Returns:
            Base64 encoded encryption key
        """
        key = Fernet.generate_key()
        return key.decode()
    
    @classmethod
    def derive_key_from_password(cls, password: str, salt: Optional[bytes] = None) -> tuple[str, bytes]:
        """Derive encryption key from password.
        
        Args:
            password: Password to derive key from
            salt: Salt for key derivation (generated if None)
            
        Returns:
            Tuple of (base64 encoded key, salt)
        """
        if salt is None:
            salt = os.urandom(16)
        
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        return key.decode(), salt
    
    def is_encryption_enabled(self) -> bool:
        """Check if encryption is enabled.
        
        Returns:
            True if encryption is enabled
        """
        return self._fernet is not None
    
    def encrypt_data(self, data: bytes) -> bytes:
        """Encrypt data.
        
        Args:
            data: Data to encrypt
            
        Returns:
            Encrypted data
            
        Raises:
            RuntimeError: If encryption is not enabled
        """
        if not self.is_encryption_enabled():
            raise RuntimeError("Encryption is not enabled")
        
        return self._fernet.encrypt(data)
    
    def decrypt_data(self, encrypted_data: bytes) -> bytes:
        """Decrypt data.
        
        Args:
            encrypted_data: Data to decrypt
            
        Returns:
            Decrypted data
            
        Raises:
            RuntimeError: If encryption is not enabled
        """
        if not self.is_encryption_enabled():
            raise RuntimeError("Encryption is not enabled")
        
        return self._fernet.decrypt(encrypted_data)
    
    def encrypt_file(self, input_file: Union[str, bytes], output_file: Optional[str] = None) -> bytes:
        """Encrypt file contents.
        
        Args:
            input_file: Path to input file or file data as bytes
            output_file: Path to output file (optional)
            
        Returns:
            Encrypted data
        """
        if isinstance(input_file, str):
            with open(input_file, 'rb') as f:
                data = f.read()
        else:
            data = input_file
        
        encrypted_data = self.encrypt_data(data)
        
        if output_file:
            with open(output_file, 'wb') as f:
                f.write(encrypted_data)
        
        return encrypted_data
    
    def decrypt_file(self, input_file: Union[str, bytes], output_file: Optional[str] = None) -> bytes:
        """Decrypt file contents.
        
        Args:
            input_file: Path to encrypted file or encrypted data as bytes
            output_file: Path to output file (optional)
            
        Returns:
            Decrypted data
        """
        if isinstance(input_file, str):
            with open(input_file, 'rb') as f:
                encrypted_data = f.read()
        else:
            encrypted_data = input_file
        
        decrypted_data = self.decrypt_data(encrypted_data)
        
        if output_file:
            with open(output_file, 'wb') as f:
                f.write(decrypted_data)
        
        return decrypted_data
    
    def get_encrypted_filename(self, original_filename: str) -> str:
        """Generate encrypted filename.
        
        Args:
            original_filename: Original filename
            
        Returns:
            Encrypted filename with .enc extension
        """
        if not self.is_encryption_enabled():
            return original_filename
        
        # Encrypt just the filename (without path)
        filename_bytes = original_filename.encode('utf-8')
        encrypted_filename = self.encrypt_data(filename_bytes)
        
        # Use base64 encoding for safe filename
        safe_filename = base64.urlsafe_b64encode(encrypted_filename).decode()
        
        # Limit length and add extension
        if len(safe_filename) > 200:
            safe_filename = safe_filename[:200]
        
        return f"{safe_filename}.enc"
    
    def decrypt_filename(self, encrypted_filename: str) -> str:
        """Decrypt encrypted filename.
        
        Args:
            encrypted_filename: Encrypted filename
            
        Returns:
            Original filename
        """
        if not self.is_encryption_enabled():
            return encrypted_filename
        
        # Remove .enc extension
        if encrypted_filename.endswith('.enc'):
            encrypted_filename = encrypted_filename[:-4]
        
        try:
            # Decode from base64
            encrypted_data = base64.urlsafe_b64decode(encrypted_filename.encode())
            
            # Decrypt
            filename_bytes = self.decrypt_data(encrypted_data)
            
            return filename_bytes.decode('utf-8')
        except Exception:
            # If decryption fails, return as-is
            return encrypted_filename
    
    def create_metadata(self, original_filename: str, original_size: int) -> dict:
        """Create metadata for encrypted file.
        
        Args:
            original_filename: Original filename
            original_size: Original file size
            
        Returns:
            Metadata dictionary
        """
        return {
            'original_filename': original_filename,
            'original_size': original_size,
            'encrypted': True,
            'encryption_version': '1.0'
        }
