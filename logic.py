import os
import requests
import dotenv
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

dotenv.load_dotenv()

BASE_URL = 'https://api.sportmonks.com'
SPORTMONKS_API_KEY = os.getenv('SPORTMONKS_API_KEY')


def load_data(endpoint: str, params: dict = {}, limit: int = None):
    data = []
    params = {
        **params,
        'api_token': SPORTMONKS_API_KEY,
    }
    url = f"{BASE_URL}/{endpoint}"
    logger.info(f"Loading data from {url}")
    response = requests.get(
        url,
        params=params,
    )
    res = response.json()
    if res.get('message'):
        logger.info(res['message'])
        if res['message'].startswith('No result(s) found'):
            return data
    data = res['data']
    page_info = res.get('pagination')
    if page_info and page_info['has_more']:
        if limit and len(data) >= limit:
            return data
        page = page_info['current_page'] + 1
        data += load_data(endpoint, params={**params, 'page': page})

    return data


def get_fixtures(start_date_end_date: tuple = None):
    endpoint = 'v3/football/fixtures'
    if start_date_end_date:
        date_start, date_end = start_date_end_date
        endpoint = f'{endpoint}/between/{date_start}/{date_end}'
    res = load_data(
        endpoint,
        params={
            'per_page': 50,
            'include': 'events;lineups;statistics;scores;periods;participants;pressure;ballCoordinates'
        }
    )

    for f in res:
        for participant in f['participants']:
            participant['fixture_id'] = f['id']
            meta = participant['meta']
            if meta['location'] == 'away':
                f['away_team_name'] = participant['name']
                f['away_team_image'] = participant['image_path']
                f['away_team_id'] = participant['id']
                if meta['winner']:
                    f['winning_team'] = participant['name']
            else:
                f['home_team_name'] = participant['name']
                f['home_team_image'] = participant['image_path']
                f['home_team_id'] = participant['id']
                if meta['winner']:
                    f['winning_team'] = participant['name']
                    f['winning_team_id'] = participant['id']

        f['home_score'] = 0
        f['away_score'] = 0
        for score in f['scores']:
            score_details = score['score']
            if score['description'] == 'CURRENT':
                if score_details['participant'] == 'home':
                    f['home_score'] = score_details['goals']
                else:
                    f['away_score'] = score_details['goals']
        f['score'] = f'{f["home_score"]} - {f["away_score"]}'

        for event in f['events']:
            event['minute'] = int(event.get('minute', 0))
            event['extra_minute'] = int(event.get('extra_minute') or 0)
            # Add minute and extra_minute to starting_at to get an event timestamp
            minutes_played = event['minute'] + event['extra_minute']
            # Parse starting_at from string to datetime
            f['starting_at'] = (
                datetime.strptime(f['starting_at'], '%Y-%m-%d %H:%M:%S')
                if not isinstance(f['starting_at'], datetime)
                else f['starting_at']
            )
            event['timestamp'] = f['starting_at'] + timedelta(minutes=minutes_played)
            event['season_id'] = f['season_id']

        for statistic in f['statistics']:
            statistic['season_id'] = f['season_id']

    yield res


def get_types():
    res = load_data('v3/core/types', params={'filter': 'populate'})
    yield res


def get_countries():
    res = load_data('v3/core/countries', params={'filter': 'populate'})
    yield res


def get_cities():
    res = load_data('v3/core/cities', params={'filter': 'populate'})
    for c in res:
        c['latitude'] = float(c['latitude'].replace(',', '')) if c['latitude'] else None
        c['longitude'] = float(c['longitude'].replace(',', '')) if c['longitude'] else None
    yield res


def get_stages():
    res = load_data(f'v3/football/stages/', params={'filter': 'populate'})
    yield res


def get_teams():
    res = load_data(f'v3/football/teams/', params={'per_page': 50, 'include': 'players'})

    yield res


def get_squads():
    teams = load_data(f'v3/football/teams/', params={'filter': 'populate'})
    team_ids = [t['id'] for t in teams]
    res = []
    for team_id in team_ids:
        squad = load_data(f'v3/football/squads/teams/{team_id}', params={'filter': 'populate'})
        res += squad

    yield res


def get_players():
    res = load_data(f'v3/football/players', params={'filter': 'populate'})
    for player in res:
        player['date_of_birth'] = (
            datetime.strptime(player['date_of_birth'], '%Y-%m-%d')
            if player['date_of_birth'] and not isinstance(player['date_of_birth'], datetime)
            else player['date_of_birth']
        )

        player['age'] = (
            datetime.today().year - player['date_of_birth'].year
            if player['date_of_birth']
            else None
        )
    yield res


def get_seasons():
    res = load_data(f'v3/football/seasons/', params={'filter': 'populate'})
    return res


def get_top_scorers(season_id: int = None):
    res = []
    seasons = [{'id': season_id}] if season_id else get_seasons()
    for season in seasons:
        season_id = season['id']
        res += load_data(f'v3/football/topscorers/seasons/{season_id}', params={'filter': 'populate'})
    yield res


def get_venues():
    res = load_data(f'v3/football/venues/', params={'filter': 'populate'})
    for v in res:
        try:
            v['latitude'] = (
                float(v['latitude'].replace(',', ''))
                if v['latitude']
                else None
            )
            v['longitude'] = (
                float(v['longitude'].replace(',', ''))
                if v['longitude']
                else None
            )
        except ValueError as e:
            v['latitude'] = None
            v['longitude'] = None
            logger.error(e)
    yield res


def get_expected():
    res = load_data(f'v3/football/expected/fixtures', params={'filters': 'populate'})
    yield res


def get_predictions():
    res = load_data(f'v3/football/predictions/probabilities', params={'filters': f'populate'})
    yield res


def get_season_statistics(season_id: int = None):
    endpoint = f'v3/football/seasons/{season_id}' if season_id else 'v3/football/seasons'
    res = load_data(endpoint, params={'per_page': 50, 'include': 'statistics;league'})
    if season_id:
        # Change single result to list for later processing
        res = [res]
    for season in res:
        # Change name to <league_name> <season_name>
        season['name'] = f"{season['league']['name']} {season['name']}"
    yield res


def full_load(pipeline):
    """Load all data"""
    # Semi-live
    pipeline.run(get_fixtures, table_name='fixtures', write_disposition='merge', primary_key='id')
    pipeline.run(get_top_scorers, table_name='top_scorers', write_disposition='merge', primary_key='id')
    pipeline.run(get_season_statistics, table_name='seasons', write_disposition='merge', primary_key='id')
    pipeline.run(get_predictions, table_name='predictions', write_disposition='merge', primary_key='id')

    # Once a day
    pipeline.run(get_types, table_name='types', write_disposition='merge', primary_key='id')
    pipeline.run(get_countries, table_name='countries', write_disposition='merge', primary_key='id')
    pipeline.run(get_cities, table_name='cities', write_disposition='merge', primary_key='id')
    pipeline.run(get_stages, table_name='stages', write_disposition='merge', primary_key='id')
    pipeline.run(get_teams, table_name='teams', write_disposition='merge', primary_key='id')
    pipeline.run(get_players, table_name='players', write_disposition='merge', primary_key='id')
    pipeline.run(get_squads, table_name='squads', write_disposition='merge', primary_key='id')
    pipeline.run(get_venues, table_name='venues', write_disposition='merge', primary_key='id')


def load_season(pipeline):
    """Only load data for current season (to speed up refreshes)"""
    seasons = get_seasons()
    current_season = [s for s in seasons if s['games_in_current_week']]
    season_id = current_season[0]['id']
    start_date_end_date = (current_season[0]['starting_at'], current_season[0]['ending_at'])
    pipeline.run(get_fixtures(start_date_end_date=start_date_end_date), table_name='fixtures', write_disposition='merge', primary_key='id')
    pipeline.run(get_top_scorers(season_id=season_id), table_name='top_scorers', write_disposition='merge', primary_key='id')
    pipeline.run(get_season_statistics(season_id=season_id), table_name='seasons', write_disposition='merge', primary_key='id')
    pipeline.run(get_predictions(), table_name='predictions', write_disposition='merge', primary_key='id')
