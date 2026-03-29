import json
import os
from datetime import datetime
from ingestion.riot_client import RiotClient

def fetch_meta_matches(n_summoners: int = 20, matches_per: int = 10) -> list:
    """
    Pull recent matches from top challenger players.
    These are the matches that define the current meta.
    """
    client = RiotClient()
    all_matches = []
    seen_match_ids = set()

    print("Fetching challenger summoners...")
    challenger_data = client.get_challenger_summoners()
    top_summoners = challenger_data['entries'][:n_summoners]
    print(f"Got {len(top_summoners)} summoners. Fetching matches...")

    for i, summoner in enumerate(top_summoners):
        puuid = summoner['puuid']
        print(f"\nSummoner {i+1}/{len(top_summoners)} | LP: {summoner['leaguePoints']}")

        match_ids = client.get_match_ids(puuid, count=matches_per)
        if not match_ids:
            print("  No matches found, skipping...")
            continue

        for match_id in match_ids:
            if match_id in seen_match_ids:
                print(f"  Skipping duplicate: {match_id}")
                continue

            match_data = client.get_match_detail(match_id)
            if match_data:
                all_matches.append(match_data)
                seen_match_ids.add(match_id)
                print(f"  Fetched {match_id} ({len(all_matches)} total)")

    return all_matches


def save_raw(matches: list, output_dir: str = "data/raw") -> str:
    """
    Save raw JSON locally.
    Never transform at ingestion time — always save raw first.
    """
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = f"{output_dir}/matches_{timestamp}.json"

    with open(filepath, 'w') as f:
        json.dump(matches, f, indent=2)

    print(f"\nSaved {len(matches)} matches to {filepath}")
    return filepath


def flatten_matches(matches: list) -> list:
    """
    Flatten nested match JSON into rows ready for BigQuery.
    One row per participant per match.
    """
    rows = []

    for match in matches:
        match_id = match['metadata']['match_id']
        game_datetime = match['info']['game_datetime']
        tft_set = match['info'].get('tft_set_number', 0)
        game_variation = match['info'].get('game_variation', 'standard')

        for participant in match['info']['participants']:
            rows.append({
                'match_id': match_id,
                'game_datetime': game_datetime,
                'tft_set': tft_set,
                'game_variation': game_variation,
                'puuid': participant['puuid'],
                'placement': participant['placement'],
                'augments': json.dumps(participant.get('augments', [])),
                'units': json.dumps([
                    {
                        'character_id': u['character_id'],
                        'tier': u.get('tier', 1),
                        'items': u.get('itemNames', [])
                    }
                    for u in participant.get('units', [])
                ]),
                'traits': json.dumps([
                    {
                        'name': t['name'],
                        'tier_current': t.get('tier_current', 0),
                        'num_units': t.get('num_units', 0)
                    }
                    for t in participant.get('traits', [])
                ]),
                'total_damage_to_players': participant.get('total_damage_to_players', 0),
                'last_round': participant.get('last_round', 0),
                'level': participant.get('level', 1),
                'ingested_at': datetime.now().isoformat()
            })

    return rows


if __name__ == "__main__":
    # Small run first — 3 summoners, 3 matches each = 9 matches max
    # Scale up once everything is confirmed working
    matches = fetch_meta_matches(n_summoners=3, matches_per=3)
    filepath = save_raw(matches)

    # Preview flattened output
    rows = flatten_matches(matches)
    print(f"\nFlattened into {len(rows)} participant rows")
    print(f"Sample row keys: {list(rows[0].keys())}")