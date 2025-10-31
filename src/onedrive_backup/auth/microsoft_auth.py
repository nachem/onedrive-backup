"""Microsoft Graph authentication handling."""

import os
import json
from typing import Optional, Dict, Any
from pathlib import Path
import msal
from azure.identity import ClientSecretCredential, InteractiveBrowserCredential
import requests

class MicrosoftGraphAuth:
    """Handle authentication for Microsoft Graph API."""
    
    def __init__(self, app_id: str, app_secret: Optional[str] = None, tenant_id: Optional[str] = None):
        """Initialize Microsoft Graph authentication.
        
        Args:
            app_id: Azure application ID
            app_secret: Azure application secret (for confidential client)
            tenant_id: Azure tenant ID (optional, defaults to common)
        """
        self.app_id = app_id
        self.app_secret = app_secret
        self.tenant_id = tenant_id or "common"
        self.token_cache_path = Path.home() / ".onedrive_backup" / "token_cache.json"
        self.token_cache_path.parent.mkdir(exist_ok=True)
        
        # Required scopes for OneDrive and SharePoint
        if self.app_secret:
            # For client credential flows, use .default scope
            self.scopes = ["https://graph.microsoft.com/.default"]
        else:
            # For interactive flows, use specific scopes
            self.scopes = [
                "https://graph.microsoft.com/Files.Read.All",
                "https://graph.microsoft.com/Sites.Read.All",
                "https://graph.microsoft.com/User.Read"
            ]
        
        self._access_token: Optional[str] = None
        self._app: Optional[msal.ClientApplication] = None
    
    def _get_msal_app(self) -> msal.ClientApplication:
        """Get MSAL application instance."""
        if self._app is None:
            # Load token cache if it exists
            cache = msal.SerializableTokenCache()
            if self.token_cache_path.exists():
                with open(self.token_cache_path, 'r') as f:
                    cache.deserialize(f.read())
            
            if self.app_secret:
                # Confidential client (app + secret)
                self._app = msal.ConfidentialClientApplication(
                    client_id=self.app_id,
                    client_credential=self.app_secret,
                    authority=f"https://login.microsoftonline.com/{self.tenant_id}",
                    token_cache=cache
                )
            else:
                # Public client (interactive login)
                self._app = msal.PublicClientApplication(
                    client_id=self.app_id,
                    authority=f"https://login.microsoftonline.com/{self.tenant_id}",
                    token_cache=cache
                )
        
        return self._app
    
    def _save_token_cache(self):
        """Save token cache to disk."""
        app = self._get_msal_app()
        if app.token_cache.has_state_changed:
            with open(self.token_cache_path, 'w') as f:
                f.write(app.token_cache.serialize())
    
    def authenticate(self, use_interactive: bool = True) -> str:
        """Authenticate and get access token.
        
        Args:
            use_interactive: Whether to use interactive authentication for public clients
            
        Returns:
            Access token string
            
        Raises:
            Exception: If authentication fails
        """
        app = self._get_msal_app()
        
        # First, try to get token silently from cache
        accounts = app.get_accounts()
        if accounts:
            result = app.acquire_token_silent(self.scopes, account=accounts[0])
            if result and "access_token" in result:
                self._access_token = result["access_token"]
                self._save_token_cache()
                return self._access_token
        
        # If silent acquisition failed, try different methods based on client type
        if self.app_secret:
            # Confidential client - use client credentials flow
            result = app.acquire_token_for_client(scopes=self.scopes)
        else:
            # Public client - use interactive or device flow
            if use_interactive:
                result = app.acquire_token_interactive(scopes=self.scopes)
            else:
                # Device code flow (useful for headless scenarios)
                flow = app.initiate_device_flow(scopes=self.scopes)
                print(f"Please go to {flow['verification_uri']} and enter the code: {flow['user_code']}")
                result = app.acquire_token_by_device_flow(flow)
        
        if "access_token" in result:
            self._access_token = result["access_token"]
            self._save_token_cache()
            return self._access_token
        else:
            error_msg = result.get("error_description", result.get("error", "Unknown authentication error"))
            raise Exception(f"Authentication failed: {error_msg}")
    
    def get_access_token(self, force_refresh: bool = False) -> str:
        """Get current access token, refreshing if necessary.
        
        Args:
            force_refresh: Force token refresh even if current token seems valid
            
        Returns:
            Access token string
        """
        if self._access_token is None or force_refresh:
            return self.authenticate(use_interactive=False)
        
        return self._access_token
    
    def get_auth_headers(self) -> Dict[str, str]:
        """Get authorization headers for API requests.
        
        Returns:
            Dictionary with authorization headers
        """
        token = self.get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
    def test_connection(self) -> bool:
        """Test if authentication is working by making a simple Graph API call.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            headers = self.get_auth_headers()
            response = requests.get(
                "https://graph.microsoft.com/v1.0/me",
                headers=headers,
                timeout=30
            )
            return response.status_code == 200
        except Exception:
            return False
    
    def clear_cache(self):
        """Clear stored token cache."""
        if self.token_cache_path.exists():
            self.token_cache_path.unlink()
        self._access_token = None
        self._app = None

    @classmethod
    def from_env(cls) -> "MicrosoftGraphAuth":
        """Create authentication instance from environment variables.
        
        Returns:
            MicrosoftGraphAuth instance
            
        Raises:
            ValueError: If required environment variables are missing
        """
        app_id = os.getenv('MICROSOFT_APP_ID')
        if not app_id:
            raise ValueError("MICROSOFT_APP_ID environment variable is required")
        
        app_secret = os.getenv('MICROSOFT_APP_SECRET')
        tenant_id = os.getenv('MICROSOFT_TENANT_ID')
        
        return cls(app_id=app_id, app_secret=app_secret, tenant_id=tenant_id)
