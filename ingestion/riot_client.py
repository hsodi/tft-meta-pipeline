import requests
import time
import os
from dotenv import load_dotenv

load_dotenv()

class RiotClient:
    """
    Wrapper around Riot TFT API
    Handles rate limiting: 20 requests/second, 100 requests/2 minutes.
    """
    PLATFORM_URL = "https://euw1.api.riotgames.com"
    REGIONAL_URL = "https://europe.api.riotgames.com"

    def __init__(self):
        self.api_key = os.getenv("RIOT_API_KEY")
        if not self.api_key:
            raise ValueError("RIOT_API_KEY not found in .env file")
        self.headers = {"X-Riot-Token": self.api_key}

    def _get(self, base_url: str, endpoint: str, params: dict = None) -> dict:
        """Make a rate-limited GET request"""
        url = f"{base_url}{endpoint}"
        time.sleep(1.2)  # Stay safely under rate limit

        response = requests.get(url, headers=self.headers, params=params)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            print(f"Rate limited. Waiting {retry_after}s...")
            time.sleep(retry_after)
            return self._get(endpoint,params)
        
        if response.status_code != 200:
            print(f"API error {response.status_code}: {response.text}")
            return None
        
        return response.json()
    
    def get_challenger_summoners(self) -> dict:
        """Get top ladder players - best source for meta data"""
        return self._get(self.PLATFORM_URL, "/tft/league/v1/challenger")
    
    # def get_summoner_by_id(self, summoner_id: str) -> dict:
    #     """Get summoner details including PUUID"""
    #     return self._get(f"/tft/summoner/v1/summoners/{summoner_id}")
    
    def get_match_ids(self, puuid: str, count: int = 20) -> list:
        """Get recent match IDs for a player."""
        return self._get(self.REGIONAL_URL, f"/tft/match/v1/matches/by-puuid/{puuid}/ids", params = {"count": count})
    
    def get_match_detail(self, match_id:str) -> dict:
        """Get full match data."""
        return self._get(self.REGIONAL_URL, f"/tft/match/v1/matches/{match_id}")
    