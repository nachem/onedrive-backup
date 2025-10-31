"""Microsoft Graph authentication handling."""

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

import msal
import requests
from azure.identity import ClientSecretCredential, InteractiveBrowserCredential

logger = logging.getLogger(__name__)

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
        self._token_expiry: Optional[float] = None  # Unix timestamp when token expires
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
            
            # Store token expiry time (with buffer for safety)
            # MSAL typically returns tokens valid for 3600 seconds (1 hour)
            expires_in = result.get("expires_in", 3600)
            self._token_expiry = time.time() + expires_in
            
            logger.info(f"✅ Obtained new access token (expires in {expires_in} seconds)")
            self._save_token_cache()
            return self._access_token
        else:
            error_msg = result.get("error_description", result.get("error", "Unknown authentication error"))
            raise Exception(f"Authentication failed: {error_msg}")
    
    def _is_token_expired(self) -> bool:
        """Check if the current access token is expired or about to expire.
        
        Returns:
            True if token is expired or will expire within 5 minutes
        """
        if self._access_token is None or self._token_expiry is None:
            return True
        
        # Add 5-minute buffer before expiry
        buffer_seconds = 300
        current_time = time.time()
        
        is_expired = current_time >= (self._token_expiry - buffer_seconds)
        
        if is_expired:
            time_left = self._token_expiry - current_time
            logger.warning(f"⚠️ Access token expired or expiring soon (time left: {time_left:.0f}s)")
        
        return is_expired
    
    def get_access_token(self, force_refresh: bool = False) -> str:
        """Get current access token, automatically refreshing if expired.
        
        This method implements automatic token refresh to prevent HTTP 401 errors
        during long-running backup operations.
        
        Args:
            force_refresh: Force token refresh even if current token seems valid
            
        Returns:
            Access token string
        """
        # Check if token needs refresh
        if force_refresh or self._is_token_expired():
            if self._access_token is not None and not force_refresh:
                logger.info("🔄 Access token expired, refreshing automatically...")
            
            # Try silent refresh first using MSAL's token cache
            app = self._get_msal_app()
            accounts = app.get_accounts()
            
            if accounts:
                result = app.acquire_token_silent(self.scopes, account=accounts[0])
                if result and "access_token" in result:
                    self._access_token = result["access_token"]
                    expires_in = result.get("expires_in", 3600)
                    self._token_expiry = time.time() + expires_in
                    logger.info(f"✅ Token refreshed automatically (expires in {expires_in}s)")
                    self._save_token_cache()
                    return self._access_token
            
            # If silent refresh failed, try full authentication
            if self.app_secret:
                # For confidential clients, use client credentials
                result = app.acquire_token_for_client(scopes=self.scopes)
                if "access_token" in result:
                    self._access_token = result["access_token"]
                    expires_in = result.get("expires_in", 3600)
                    self._token_expiry = time.time() + expires_in
                    logger.info(f"✅ Token refreshed via client credentials (expires in {expires_in}s)")
                    self._save_token_cache()
                    return self._access_token
                else:
                    error = result.get("error_description", "Unknown error")
                    logger.error(f"❌ Failed to refresh token: {error}")
                    raise Exception(f"Token refresh failed: {error}")
            else:
                # For public clients, need to re-authenticate interactively
                logger.warning("⚠️ Token expired and cannot refresh automatically (interactive login required)")
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
