import json
import re
from datetime import datetime
from collections import defaultdict

# ============================================
# VARIABLES GLOBALES
# ============================================

COUNTRY_IMAGES = {
    'spain': 'img/es.png',
    'england': 'img/en.png',
    'italy': 'img/it.png',
    'germany': 'img/de.png',
    'france': 'img/fr.png',
    'netherlands': 'img/nl.png',
    'portugal': 'img/pt.png',
    'turkey': 'img/tr.png',
    'argentina': 'img/ar.png',
    'brazil': 'img/br.png',
    'colombia': 'img/col.png',
    'chile': 'img/cl.png',
    'uruguay': 'img/uy.png',
    'paraguay': 'img/py.png',
    'ecuador': 'img/ec.png',
    'peru': 'img/pe.png',
    'mexico': 'img/mx.png',
    'arabia': 'img/sa.png',
    'usa': 'img/usa.png',
    'nba': 'img/nba.png',
    'mlb': 'img/mlb.png',
    'wwe': 'img/wwe.png',
    'ufc': 'img/ufc.png',
    'default': 'img/default.png'
}

# Mapeo de ligas a países
LEAGUE_TO_COUNTRY = {
    'LaLiga': 'spain', 'La Liga': 'spain', 'LaLiga 2:': 'spain',
    'Premier League': 'england', 'Premier League:': 'england', 'Championship': 'england',
    'Bundesliga': 'germany', 'Bundesliga 2': 'germany', 'DFB Pokal': 'germany',
    'Serie A': 'italy', 'Serie A:': 'italy',
    'Ligue 1': 'france', 'Ligue 2': 'france',
    'Eredivisie': 'netherlands',
    'Primeira Liga': 'portugal', 'Taça de Portugal': 'portugal', 'Copa de Portugal': 'portugal',
    'Liga MX': 'mexico',
    'Liga 1': 'peru', 'Primera Division': 'peru',
    'Copa do Brasil': 'brazil', 'Brasileirão': 'brazil',
    'Liga Profesional': 'argentina', 'Primera División': 'argentina', 'Futbol Argentino': 'argentina',
    'Liga BetPlay': 'colombia', 'Liga Betplay': 'colombia', 'Primera A': 'colombia',
    'Liga de Primera': 'chile', 'Futbol Chileno': 'chile',
    'Liga Pro': 'ecuador',
    'Turkish Cup': 'turkey', 'Süper Lig': 'turkey',
    'Pro League': 'arabia', 
    'MLB': 'mlb', 'NBA': 'nba', 'WWE': 'wwe',
}

SEPARATORS = [' vs ', ' v ', ' - ', ' vs. ', ' versus ']

CONFIG = {
    'min_confidence_for_image': 0.7,
    'input_file': 'scraper_output.json',
    'output_file': 'matches.json'
}

# ============================================
# CARGAR BASE DE DATOS
# ============================================

def load_teams():
    try:
        with open('teams.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print("❌ Error: No se encuentra teams.json")
        return {}

TEAMS_DB = load_teams()

# ============================================
# DETECCIÓN DE DEPORTE POR LIGA
# ============================================

def get_sport_from_liga(liga):
    liga_lower = liga.lower()
    if 'nba' in liga_lower:
        return 'nba'
    if 'mlb' in liga_lower:
        return 'mlb'
    if 'nfl' in liga_lower:
        return 'nfl'
    if 'wwe' in liga_lower:
        return 'wwe'
    return 'soccer'

def validate_sport_by_liga(team1, team2, liga):
    sport = get_sport_from_liga(liga)
    if sport in ['mlb', 'nba', 'nfl', 'wwe']:
        return True
    
    team1_lower = team1.lower()
    team2_lower = team2.lower()
    
    nba_keywords = ['lakers', 'warriors', 'celtics', 'bulls', 'heat', 'bucks', 'suns', 'nuggets', 
                    'mavericks', 'spurs', 'blazers', 'trail blazers', 'knicks', 'nets', 'raptors']
    mlb_keywords = ['tigers', 'red sox', 'orioles', 'guardians', 'blue jays', 'twins', 'rays',
                    'rockies', 'mets', 'phillies', 'braves', 'nationals', 'white sox', 'pirates',
                    'brewers', 'angels', 'royals', 'cubs', 'dodgers', 'marlins', 'giants']
    
    for kw in nba_keywords:
        if kw in team1_lower or kw in team2_lower:
            print(f"⚠️ Deporte incompatible: {team1} vs {team2} (liga: {liga} pero parece NBA)")
            return False
    
    for kw in mlb_keywords:
        if kw in team1_lower or kw in team2_lower:
            print(f"⚠️ Deporte incompatible: {team1} vs {team2} (liga: {liga} pero parece MLB)")
            return False
    
    return True

# ============================================
# UTILIDADES
# ============================================

def normalize(text):
    if not text:
        return ''
    text = text.lower()
    accents = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'ü': 'u', 'ñ': 'n'
    }
    for accented, unaccented in accents.items():
        text = text.replace(accented, unaccented)
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# ============================================
# BUSCAR EQUIPO (MODIFICADO CON DESAMBIGUACIÓN POR LIGA)
# ============================================

def find_team(query, liga=None):
    if not TEAMS_DB:
        return {'name': query, 'country': 'unknown', 'confidence': 0}
    
    normalized_query = normalize(query)
    
    # Obtener país esperado de la liga (si se proporcionó)
    expected_country = None
    if liga:
        expected_country = LEAGUE_TO_COUNTRY.get(liga, None)
    
    # PRIMERO: Buscar equipos que coincidan con el país de la liga
    if expected_country:
        for slug, team in TEAMS_DB.items():
            if team.get('country') == expected_country:
                if normalize(team['name']) == normalized_query:
                    return {'name': team['name'], 'country': team['country'], 'confidence': 1.0}
                for alias in team.get('aliases', []):
                    if normalize(alias) == normalized_query:
                        return {'name': team['name'], 'country': team['country'], 'confidence': 0.95}
        
        # Búsqueda parcial en equipos del país correcto
        best_match = None
        best_score = 0
        for slug, team in TEAMS_DB.items():
            if team.get('country') == expected_country:
                if (normalize(team['name']) in normalized_query or 
                    normalized_query in normalize(team['name'])):
                    score = 0.85
                    if score > best_score:
                        best_score = score
                        best_match = {'name': team['name'], 'country': team['country'], 'confidence': score}
        
        if best_match:
            return best_match
    
    # SEGUNDO: Buscar en todos los equipos (sin filtro de país)
    for slug, team in TEAMS_DB.items():
        if normalize(team['name']) == normalized_query:
            return {'name': team['name'], 'country': team['country'], 'confidence': 0.9}
        for alias in team.get('aliases', []):
            if normalize(alias) == normalized_query:
                return {'name': team['name'], 'country': team['country'], 'confidence': 0.85}
    
    # Búsqueda parcial final
    best_match = None
    best_score = 0
    for slug, team in TEAMS_DB.items():
        if (normalize(team['name']) in normalized_query or 
            normalized_query in normalize(team['name'])):
            score = 0.7
            if score > best_score:
                best_score = score
                best_match = {'name': team['name'], 'country': team['country'], 'confidence': score}
    
    return best_match or {'name': query, 'country': 'unknown', 'confidence': 0}

# ============================================
# OBTENER LOGO POR PAÍS O LIGA
# ============================================

def get_image(liga, country=None):
    if country and country in COUNTRY_IMAGES:
        return COUNTRY_IMAGES[country]
    
    for liga_key, country_value in LEAGUE_TO_COUNTRY.items():
        if liga_key.lower() in liga.lower():
            return COUNTRY_IMAGES.get(country_value, COUNTRY_IMAGES['default'])
    
    return COUNTRY_IMAGES['default']

# ============================================
# RESOLVER PARTIDO (MODIFICADO)
# ============================================

def resolve_match(match_text, liga=''):
    clean = match_text.replace('.', '').strip()
    
    parts = None
    for sep in SEPARATORS:
        if sep in clean:
            parts = clean.split(sep)
            break
    
    if not parts or len(parts) != 2:
        return None
    
    team1_name = parts[0].strip()
    team2_name = parts[1].strip()
    
    # Pasar la liga a find_team para desambiguar
    team1 = find_team(team1_name, liga)
    team2 = find_team(team2_name, liga)
    
    if not validate_sport_by_liga(team1['name'], team2['name'], liga):
        print(f"⚠️ Deporte incompatible: {team1['name']} vs {team2['name']} - ignorando")
        return None
    
    same_country = (team1['country'] != 'unknown' and team1['country'] == team2['country'])
    
    if same_country and team1['country'] != 'unknown':
        image = COUNTRY_IMAGES.get(team1['country'], COUNTRY_IMAGES['default'])
    else:
        image = get_image(liga, None)
    
    return {
        'team1': team1['name'],
        'team2': team2['name'],
        'team1_country': team1['country'],
        'team2_country': team2['country'],
        'same_country': same_country,
        'confidence': round((team1['confidence'] + team2['confidence']) / 2, 2),
        'image': image,
        'sport': get_sport_from_liga(liga)
    }

# ============================================
# GENERAR matches.json UNIFICADO
# ============================================

def generate_matches_json(input_file=None, output_file=None):
    input_file = input_file or CONFIG['input_file']
    output_file = output_file or CONFIG['output_file']
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            raw_matches = json.load(f)
    except FileNotFoundError:
        print(f"❌ No se encuentra {input_file}")
        print(f"📝 Ejecuta primero: python scraper.py")
        return
    
    print(f"📊 Procesando {len(raw_matches)} eventos sin procesar...")
    
    unified = {}
    partidos_ignorados = 0
    
    for item in raw_matches:
        match_text = item.get('equipos') or item.get('match') or item.get('title')
        if not match_text:
            continue
        
        liga = item.get('liga', '').replace(':', '')
        resolved = resolve_match(match_text, liga)
        
        if not resolved:
            partidos_ignorados += 1
            continue
        
        fecha = item.get('hora_utc', '')[:10] if item.get('hora_utc') else ''
        key = f"{resolved['team1']} vs {resolved['team2']}|{fecha}"
        
        if key not in unified:
            unified[key] = {
                'hora_utc': item.get('hora_utc', ''),
                'liga': liga,
                'equipos': f"{resolved['team1']} vs {resolved['team2']}",
                'team1': resolved['team1'],
                'team2': resolved['team2'],
                'team1_country': resolved['team1_country'],
                'team2_country': resolved['team2_country'],
                'same_country': resolved['same_country'],
                'confidence': resolved['confidence'],
                'image': resolved['image'],
                'sport': resolved.get('sport', 'soccer'),
                'canales': []
            }
        else:
            if liga and not unified[key]['liga']:
                unified[key]['liga'] = liga
        
        for canal in item.get('canales', []):
            url = canal.get('url', '')
            if not url:
                continue
            
            exists = False
            for existing in unified[key]['canales']:
                if existing.get('url') == url:
                    exists = True
                    break
            
            if not exists:
                unified[key]['canales'].append({
                    'nombre': canal.get('nombre', 'Canal'),
                    'url': url,
                    'calidad': canal.get('calidad', 'HD')
                })
    
    matches_list = list(unified.values())
    matches_list.sort(key=lambda x: x.get('hora_utc', ''))
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(matches_list, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Generado {output_file} con {len(matches_list)} partidos unificados")
    if partidos_ignorados > 0:
        print(f"⚠️ Partidos ignorados por deporte incompatible: {partidos_ignorados}")
    
    deportes = defaultdict(int)
    for m in matches_list:
        deportes[m.get('sport', 'soccer')] += 1
    
    print(f"📊 Desglose por deporte:")
    for deporte, count in deportes.items():
        print(f"   - {deporte}: {count} partidos")
    
    total_canales = sum(len(m['canales']) for m in matches_list)
    print(f"📡 Total de canales únicos: {total_canales}")
    
    return matches_list

# ============================================
# CLI
# ============================================

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("""
🏆 SPORTS ENGINE

Comandos:
  python engine.py batch      → Generar matches.json unificado
  python engine.py resolve "Real Madrid vs Barcelona" → Probar un partido
        """)
    elif sys.argv[1] == 'resolve':
        if len(sys.argv) < 3:
            print("Uso: python engine.py resolve \"Equipo A vs Equipo B\"")
        else:
            result = resolve_match(sys.argv[2])
            print(json.dumps(result, indent=2, ensure_ascii=False))
    elif sys.argv[1] == 'batch':
        generate_matches_json()
    else:
        print("Comando no reconocido. Usa 'resolve' o 'batch'")
