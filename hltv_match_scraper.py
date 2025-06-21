import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from random import choice, uniform
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# List of User-Agents to rotate
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
]

# List of target teams to filter matches
TARGET_TEAMS = {
    'Legacy', 'TYLOO', 'BetBoom', 'Nemiga', 'Lynn Vision', 'OG', 'B8', 'Falcons', 'HEROIC', 'FURIA',
    'FaZe', '3DMAX', 'paiN', 'M80', 'Virtus.pro', 'MIBR', 'Aurora', 'The MongolZ', 'G2', 'Vitality',
    'MOUZ', 'Spirit', 'Liquid', 'Natus Vincere'
}

# Template for player data to ensure consistent columns
PLAYER_DATA_TEMPLATE = {
    'Team': 'N/A',
    'Map': 'N/A',
    'Map Won': 'N/A',
    'Picks': 'N/A',
    'Bans': 'N/A',
    'Player': 'N/A',
    'Nationality': 'Unknown',
    'Kills': 'N/A',
    'Assists': 'N/A',
    'Deaths': 'N/A',
    'KAST': 'N/A',
    'K-D Diff': 'N/A',
    'ADR': 'N/A',
    'FK Diff': 'N/A',
    'Rating': 'N/A',
    'Team Total Score': 'N/A',
    'Team CT Half': 'N/A',
    'Team T Half': 'N/A',
    'Team Rating': 'N/A',
    'First Kills': 'N/A',
    'Clutches Won': 'N/A',
    'Match URL': 'N/A',
    'GameID': 'N/A'
}

# Function to create a session with retries
def create_session():
    session = cloudscraper.create_scraper()
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

# Function to clean text
def clean_text(text, remove_paren=False):
    if not isinstance(text, str):
        text = str(text)
    text = text.strip()
    if remove_paren:
        text = re.sub(r'\s*\([^)]+\)', '', text)
    return text

# Function to extract GameID from match stats URL
def extract_game_id(url):
    match = re.search(r'/mapstatsid/(\d+)/', url)
    return match.group(1) if match else 'N/A'

# Function to find best matching team name
def find_best_team_match(short_name, full_team_names):
    if not short_name or not isinstance(short_name, str):
        return 'N/A'
    short_name = short_name.lower().strip()
    for full_name in full_team_names:
        if short_name in full_name.lower():
            return full_name
    return short_name

# Function to scrape match stats, map, picks/bans, and breakdown
def scrape_match_stats(match_stats_url, session, match_url):
    headers = {
        'User-Agent': choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.hltv.org/',
        'Connection': 'keep-alive',
    }
    try:
        response = session.get(match_stats_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract GameID
        game_id = extract_game_id(match_stats_url)

        # Extract map name
        map_name = 'N/A'
        small_text_div = soup.find('div', class_='small-text')
        if small_text_div and small_text_div.find('span', class_='bold', string='Map'):
            next_sibling = small_text_div.find_next_sibling(string=True)
            if next_sibling:
                map_name = clean_text(next_sibling)

        # Extract score breakdown, team rating, first kills, and clutches won
        score_data = {}
        score_container = soup.find('div', class_='match-info-box-col')
        team_left_name = 'N/A'
        team_right_name = 'N/A'
        total_scores = ['N/A', 'N/A']
        
        if score_container:
            team_left_div = score_container.find('div', class_='team-left')
            team_right_div = score_container.find('div', class_='team-right')
            
            if team_left_div:
                team_left_logo = team_left_div.find('img', class_='team-logo')
                if team_left_logo and 'title' in team_left_logo.attrs:
                    team_left_name = clean_text(team_left_logo['title'])
                else:
                    team_left_text = team_left_div.find('div', class_='bold')
                    if team_left_text:
                        team_left_name = clean_text(team_left_text.text)
                score_elem = team_left_div.find('div', class_='bold')
                if score_elem:
                    total_scores[0] = clean_text(score_elem.text)
            
            if team_right_div:
                team_right_logo = team_right_div.find('img', class_='team-logo')
                if team_right_logo and 'title' in team_right_logo.attrs:
                    team_right_name = clean_text(team_right_logo['title'])
                else:
                    team_right_text = team_right_div.find('div', class_='bold')
                    if team_right_text:
                        team_right_name = clean_text(team_right_text.text)
                score_elem = team_right_div.find('div', class_='bold')
                if score_elem:
                    total_scores[1] = clean_text(score_elem.text)

            score_data[team_left_name] = {'Total Score': total_scores[0]}
            score_data[team_right_name] = {'Total Score': total_scores[1]}

            match_info_rows = score_container.find_all('div', class_='match-info-row')
            for row in match_info_rows:
                bold_text = row.find('div', class_='bold')
                right_text = row.find('div', class_='right')
                if bold_text and right_text:
                    label = clean_text(bold_text.text)
                    values = clean_text(right_text.text)
                    
                    if label == 'Breakdown':
                        half_matches = re.findall(r'\(\s*(\d+)\s*:\s*(\d+)\s*\)', values)
                        if len(half_matches) == 2:
                            score_data[team_left_name].update({
                                'CT Half': half_matches[0][0],
                                'T Half': half_matches[1][0]
                            })
                            score_data[team_right_name].update({
                                'CT Half': half_matches[1][1],
                                'T Half': half_matches[0][1]
                            })
                    elif label == 'Team rating 2.1':
                        ratings = values.split(' : ')
                        if len(ratings) == 2:
                            score_data[team_left_name]['Team Rating'] = ratings[0]
                            score_data[team_right_name]['Team Rating'] = ratings[1]
                    elif label == 'First kills':
                        kills = values.split(' : ')
                        if len(kills) == 2:
                            score_data[team_left_name]['First Kills'] = kills[0]
                            score_data[team_right_name]['First Kills'] = kills[1]
                    elif label == 'Clutches won':
                        clutches = values.split(' : ')
                        if len(clutches) == 2:
                            score_data[team_left_name]['Clutches Won'] = clutches[0]
                            score_data[team_right_name]['Clutches Won'] = clutches[1]

        # Determine winning team
        winning_team = 'N/A'
        if score_data:
            team1, team2 = list(score_data.keys())
            score1 = int(score_data[team1]['Total Score']) if score_data[team1]['Total Score'].isdigit() else 0
            score2 = int(score_data[team2]['Total Score']) if score_data[team2]['Total Score'].isdigit() else 0
            winning_team = team1 if score1 > score2 else team2 if score2 > score1 else 'N/A'

        # Extract map picks and bans from match page
        picks_bans_data = {team_left_name: {'Picks': [], 'Bans': []}, team_right_name: {'Picks': [], 'Bans': []}}
        try:
            headers['Referer'] = match_stats_url
            match_response = session.get(match_url, headers=headers, timeout=15)
            match_response.raise_for_status()
            match_soup = BeautifulSoup(match_response.text, 'html.parser')
            veto_box = match_soup.find('div', class_='veto-box')
            if veto_box:
                veto_text = veto_box.get_text(separator='|').replace('\n', '').replace('  ', '')
                veto_lines = veto_text.split('|')
                for line in veto_lines:
                    line = line.strip()
                    pick_ban_match = re.match(r'(\d+)\.\s*([^:]+?)\s*(picked|removed)\s*(\S+)', line)
                    if pick_ban_match:
                        _, team, action, map_ = pick_ban_match.groups()
                        team = find_best_team_match(team, [team_left_name, team_right_name])
                        if team != 'N/A':
                            if action == 'picked':
                                picks_bans_data[team]['Picks'].append(map_)
                            elif action == 'removed':
                                picks_bans_data[team]['Bans'].append(map_)
                # Handle leftover map
                for line in veto_lines:
                    leftover_match = re.match(r'(\S+)\s*was left over', line)
                    if leftover_match and map_name != 'N/A':
                        map_ = leftover_match.group(1)
                        if map_ == map_name:
                            for team in picks_bans_data:
                                if map_name not in picks_bans_data[team]['Picks'] and map_name not in picks_bans_data[team]['Bans']:
                                    picks_bans_data[team]['Picks'].append(map_name)
                                    break
        except Exception as e:
            logging.error(f"Failed to fetch match page {match_url}: {e}")

        # Extract player stats
        match_data = []
        stats_tables = soup.find_all('table', class_='stats-table totalstats')
        logging.info(f"Found {len(stats_tables)} stats tables for {match_stats_url}")
        
        for table in stats_tables:
            team_name_elem = table.find('th', class_='st-teamname')
            team_name = clean_text(team_name_elem.find('img')['alt']) if team_name_elem and team_name_elem.find('img') else 'N/A'
            team_name = find_best_team_match(team_name, [team_left_name, team_right_name])
            logging.info(f"Processing team: {team_name}")
            
            tbody = table.find('tbody')
            if not tbody:
                logging.warning(f"No tbody found in stats table for team {team_name}")
                continue
                
            rows = tbody.find_all('tr')
            logging.info(f"Found {len(rows)} player rows for team {team_name}")
            
            for row in rows:
                player_data = PLAYER_DATA_TEMPLATE.copy()
                player_data['Team'] = team_name
                player_data['Map'] = map_name
                player_data['Map Won'] = 'Yes' if team_name == winning_team else 'No'
                player_data['Picks'] = '; '.join(picks_bans_data.get(team_name, {}).get('Picks', [])) or 'N/A'
                player_data['Bans'] = '; '.join(picks_bans_data.get(team_name, {}).get('Bans', [])) or 'N/A'
                player_data['Match URL'] = match_url
                player_data['GameID'] = game_id
                
                player_cell = row.find('td', class_='st-player')
                if not player_cell or not player_cell.find('a'):
                    logging.warning(f"No player cell found in row for team {team_name}")
                    continue
                    
                player_name = clean_text(player_cell.find('a').text)
                nationality_elem = player_cell.find('img', class_='flag')
                nationality = clean_text(nationality_elem['title']) if nationality_elem else 'Unknown'
                
                player_data.update({
                    'Player': player_name,
                    'Nationality': nationality,
                    'Kills': clean_text(row.find('td', class_='st-kills').text, remove_paren=True) if row.find('td', class_='st-kills') else 'N/A',
                    'Assists': clean_text(row.find('td', class_='st-assists').text, remove_paren=True) if row.find('td', class_='st-assists') else 'N/A',
                    'Deaths': clean_text(row.find('td', class_='st-deaths').text) if row.find('td', class_='st-deaths') else 'N/A',
                    'KAST': clean_text(row.find('td', class_='st-kdratio').text) if row.find('td', class_='st-kdratio') else 'N/A',
                    'K-D Diff': clean_text(row.find('td', class_='st-kddiff').text) if row.find('td', class_='st-kddiff') else 'N/A',
                    'ADR': clean_text(row.find('td', class_='st-adr').text) if row.find('td', class_='st-adr') else 'N/A',
                    'FK Diff': clean_text(row.find('td', class_='st-fkdiff').text) if row.find('td', class_='st-fkdiff') else 'N/A',
                    'Rating': clean_text(row.find('td', class_='st-rating').text) if row.find('td', class_='st-rating') else 'N/A',
                    'Team Total Score': score_data.get(team_name, {}).get('Total Score', 'N/A'),
                    'Team CT Half': score_data.get(team_name, {}).get('CT Half', 'N/A'),
                    'Team T Half': score_data.get(team_name, {}).get('T Half', 'N/A'),
                    'Team Rating': score_data.get(team_name, {}).get('Team Rating', 'N/A'),
                    'First Kills': score_data.get(team_name, {}).get('First Kills', 'N/A'),
                    'Clutches Won': score_data.get(team_name, {}).get('Clutches Won', 'N/A')
                })
                
                match_data.append(player_data)
                logging.info(f"Added player data for {player_name} in team {team_name}")
        
        return match_data
    
    except Exception as e:
        logging.error(f"Failed to fetch or parse match stats {match_stats_url}: {e}")
        return []

# Function to scrape match links from a results page
def scrape_match_links(results_url, session):
    headers = {
        'User-Agent': choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.hltv.org/',
        'Connection': 'keep-alive',
    }
    try:
        logging.info(f"Fetching results page: {results_url}")
        response = session.get(results_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        match_links = []
        for match_div in soup.find_all('a', class_='a-reset'):
            href = match_div.get('href')
            if href and '/matches/' in href and not '/stats/matches/' in href:
                match_url = 'https://www.hltv.org' + href
                try:
                    logging.info(f"Checking match page: {match_url}")
                    match_response = session.get(match_url, headers=headers, timeout=15)
                    match_response.raise_for_status()
                    match_soup = BeautifulSoup(match_response.text, 'html.parser')

                    team_name_divs = match_soup.find_all('div', class_='teamName')
                    team_names = {clean_text(div.text).strip().lower() for div in team_name_divs}
                    normalized_target_teams = {team.lower() for team in TARGET_TEAMS}
                    
                    if any(team_name in normalized_target_teams for team_name in team_names):
                        logging.info(f"Match involves target team(s): {team_names}")
                        match_links.append(match_url)
                    else:
                        logging.info(f"No target teams found in: {team_names}")
                except Exception as e:
                    logging.error(f"Error fetching match page {match_url}: {e}")
        return match_links

    except Exception as e:
        logging.error(f"Error fetching results page {results_url}: {e}")
        return []

# Function to get stats page URL from match page
def get_stats_page_url(match_url, session):
    headers = {
        'User-Agent': choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.hltv.org/results',
        'Connection': 'keep-alive',
    }
    try:
        response = session.get(match_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        stats_link = soup.find('a', href=re.compile(r'/stats/matches/mapstatsid/\d+/.+'))
        if stats_link:
            return 'https://www.hltv.org' + stats_link['href']
        return None

    except Exception as e:
        logging.error(f"Error fetching match page {match_url}: {e}")
        return None

# Main function to scrape matches
def main():
    base_url = 'https://www.hltv.org/results?offset='
    session = create_session()
    all_match_data = []
    
    for offset in range(0, 200, 100):
        results_url = f'{base_url}{offset}'
        logging.info(f"Scraping results page: {results_url}")
        
        match_links = scrape_match_links(results_url, session)
        logging.info(f"Found {len(match_links)} matches on the results page.")

        for match_url in match_links:
            logging.info(f"Processing match: {match_url}")
            stats_url = get_stats_page_url(match_url, session)
            if stats_url:
                logging.info(f"Fetching stats from: {stats_url}")
                match_data = scrape_match_stats(stats_url, session, match_url)
                all_match_data.extend(match_data)
            else:
                logging.warning(f"No stats page found for {match_url}, skipping.")
            time.sleep(uniform(3, 7))  # Random delay between 3-7 seconds
    
    # Save to CSV
    if all_match_data:
        df = pd.DataFrame(all_match_data)
        df.to_csv('hltv_match_stats.csv', index=False)
        logging.info("Data saved to hltv_match_stats.csv")
    else:
        logging.warning("No data collected.")

if __name__ == '__main__':
    main()