import json
import re
from datetime import datetime, timedelta
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
    'bolivia': 'img/bo.png',
    'nba': 'img/nba.png',
    'mlb': 'img/mlb.png',
    'wwe': 'img/wwe.png',
    'ufc': 'img/ufc.png',
    'box': 'img/box.png',
    'nhl': 'img/nhl.png',
    'nfl': 'img/nfl.png',
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
    'Liga MX': 'mexico', 'Liga de Expansión MX': 'mexico',
    'Liga 1': 'peru', 'Primera Division': 'peru',
    'Copa do Brasil': 'brazil', 'Brasileirão': 'brazil',
    'Liga Profesional': 'argentina', 'Primera División': 'argentina', 'Futbol Argentino': 'argentina',
    'Liga BetPlay': 'colombia', 'Liga Betplay': 'colombia', 'Primera A': 'colombia',
    'Liga de Primera': 'chile', 'Futbol Chileno': 'chile',
    'Liga Pro': 'ecuador',
    'Turkish Cup': 'turkey', 'Süper Lig': 'turkey',
    'Pro League': 'arabia', 
    'MLS': 'usa', 'Major League Soccer': 'usa',
    'MLB': 'mlb', 'NBA': 'nba', 'WWE': 'wwe', 'UFC': 'ufc',
    'boxing': 'box', 'Boxeo': 'box',
    'NHL': 'nhl', 'nhl': 'nhl', 'NFL': 'nfl',
}

# ============================================
# DURACIÓN DE PARTIDOS POR DEPORTE (en minutos)
# ============================================

SPORT_DURATION = {
    'soccer': 150,      # 90' + 15' entretiempo + 15' adiciones (2 horas)
    'nba': 195,         # 48' + descansos + tiempos muertos (~3 horas)
    'mlb': 195,         # 9 entradas (~3 horas)
    'nfl': 195,         # 60' + pausas (~3.25 horas)
    'ufc': 300,         # Evento completo (~5 horas)
    'wwe': 180,         # Evento completo (~3 horas)
    'box': 300,         # Velada completa (~5 horas)
    'default': 120      # Por defecto: 2 horas
}

# ============================================
# SEPARADORES MEJORADOS (MANEJA GUIONES INTELIGENTEMENTE)
# ============================================

def split_match_text(text):
    """
    Divide el texto del partido en equipo1 y equipo2.
    Maneja intelligentemente diferentes separadores incluyendo guiones.
    """
    if not text:
        return None, None
    
    # Limpiar texto
    clean = text.strip()
    
    # 1. Probar con separadores comunes (priorizando los más específicos)
    separators = [
        ' vs ', ' v ', ' vs. ', ' versus ',
        ' - ', ' – ', ' — ',  # Guiones con espacios
        ' -', '- ',           # Guiones con espacio solo a un lado
    ]
    
    for sep in separators:
        if sep in clean:
            parts = clean.split(sep, 1)
            if len(parts) == 2:
                return parts[0].strip(), parts[1].strip()
    
    # 2. Buscar guiones con espacios alrededor
    matches = list(re.finditer(r'(?<=\s)-(?=\s)|(?<=^)-(?=\s)|(?<=\s)-(?=$)', clean))
    
    if matches:
        sep_pos = matches[-1].start()
        team1 = clean[:sep_pos].strip()
        team2 = clean[sep_pos + 1:].strip()
        if team1 and team2:
            return team1, team2
    
    # 3. Último recurso: un solo guión que no sea nombre compuesto
    if clean.count('-') == 1 and ' - ' not in clean:
        parts = clean.split('-', 1)
        if len(parts) == 2 and parts[0].strip() and parts[1].strip():
            compound_names = ['colo-colo', 'villa-lobos', 'real-sociedad', 'athletic-bilbao']
            text_lower = clean.lower()
            is_compound = any(name in text_lower for name in compound_names)
            if not is_compound:
                return parts[0].strip(), parts[1].strip()
    
    return None, None

SEPARATORS = [' vs ', ' v ', ' - ', ' vs. ', ' versus ']

CONFIG = {
    'min_confidence_for_image': 0.7,
    'input_file': 'scraper_output.json',
    'output_file': 'matches.json'
}

# ============================================
# CARGAR BASE DE DATOS (CORREGIDO)
# ============================================

def load_teams():
    """Carga equipos desde múltiples archivos JSON (soporta listas y diccionarios)"""
    teams_db = {}
    
    # 1. Cargar teams.json (fútbol general)
    try:
        with open('teams.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Si es lista, convertir a diccionario
            if isinstance(data, list):
                for team in data:
                    if 'name' in team:
                        slug = team['name'].lower().replace(' ', '_')
                        teams_db[slug] = team
            elif isinstance(data, dict):
                teams_db.update(data)
            print(f"✅ Cargados {len(teams_db)} equipos de teams.json")
    except FileNotFoundError:
        print("⚠️ No se encuentra teams.json")
    except Exception as e:
        print(f"⚠️ Error cargando teams.json: {e}")
    
    # 2. Cargar mlb_teams.json (béisbol)
    try:
        with open('mlb_teams.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
            mlb_teams = []
            
            # Extraer lista de equipos (puede ser lista directa o con clave 'mlb_teams')
            if isinstance(data, list):
                mlb_teams = data
            elif isinstance(data, dict) and 'mlb_teams' in data:
                mlb_teams = data['mlb_teams']
            elif isinstance(data, dict):
                mlb_teams = list(data.values())
            
            for team in mlb_teams:
                if 'name' in team:
                    slug = team['name'].lower().replace(' ', '_')
                    team['sport'] = 'mlb'
                    team['country'] = 'usa'
                    if 'aliases' not in team:
                        team['aliases'] = []
                    name = team.get('name', '')
                    if name.startswith('Los '):
                        team['aliases'].append(name[4:])
                    elif name.startswith('Las '):
                        team['aliases'].append(name[4:])
                    teams_db[slug] = team
            print(f"✅ Cargados {len(mlb_teams)} equipos de mlb_teams.json")
    except FileNotFoundError:
        print("⚠️ No se encuentra mlb_teams.json")
    except Exception as e:
        print(f"⚠️ Error cargando mlb_teams.json: {e}")
    
    # 3. Cargar nba_teams.json (baloncesto)
    try:
        with open('nba_teams.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
            nba_teams = []
            
            # Extraer lista de equipos
            if isinstance(data, list):
                nba_teams = data
            elif isinstance(data, dict) and 'nba_teams' in data:
                nba_teams = data['nba_teams']
            elif isinstance(data, dict):
                nba_teams = list(data.values())
            
            for team in nba_teams:
                if 'name' in team:
                    slug = team['name'].lower().replace(' ', '_')
                    team['sport'] = 'nba'
                    team['country'] = 'usa'
                    if 'aliases' not in team:
                        team['aliases'] = []
                    # Agregar alias sin ciudad
                    name = team.get('name', '')
                    if ' ' in name:
                        parts = name.split(' ', 1)
                        if len(parts) == 2 and parts[1] not in team['aliases']:
                            team['aliases'].append(parts[1])
                    teams_db[slug] = team
            print(f"✅ Cargados {len(nba_teams)} equipos de nba_teams.json")
    except FileNotFoundError:
        print("⚠️ No se encuentra nba_teams.json")
    except Exception as e:
        print(f"⚠️ Error cargando nba_teams.json: {e}")
    
    print(f"📊 Total de equipos cargados: {len(teams_db)}")
    return teams_db

TEAMS_DB = load_teams()

# ============================================
# NORMALIZACIÓN DE EVENTOS ESPECIALES
# ============================================

def normalizar_evento_especial(match_text, liga):
    """Normaliza eventos UFC/WWE por palabras exactas"""
    if not match_text:
        return match_text
    
    if 'UFC' not in liga and 'WWE' not in liga:
        return match_text
    
    texto_limpio = match_text.lower()
    palabras_eliminar = [
        'preliminares', 'prelims', 'early', 'fight night', 'ufc', 'wwe',
        'main card', 'espn', 'ppv', 'live', 'results', 'highlights',
        'post fight', 'ceremonial', 'weigh-in', 'predictions'
    ]
    
    for palabra in palabras_eliminar:
        texto_limpio = re.sub(rf'\b{palabra}\b', '', texto_limpio)
    
    texto_limpio = re.sub(r'[^\w\s]', '', texto_limpio)
    texto_limpio = re.sub(r'\s+', ' ', texto_limpio).strip()
    
    return texto_limpio

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
    if 'ufc' in liga_lower:
        return 'ufc'
    if 'box' in liga_lower or 'boxeo' in liga_lower:
        return 'box'
    return 'soccer'

def validate_sport_by_liga(team1, team2, liga):
    sport = get_sport_from_liga(liga)
    
    if sport in ['mlb', 'nba', 'nfl', 'wwe', 'ufc', 'box']:
        return True
    
    team1_lower = team1.lower()
    team2_lower = team2.lower()
    liga_lower = liga.lower()
    
    # Filtrar Rugby
    rugby_keywords = [
        'rugby', 'rugbi', 'six nations', 'seis naciones', 'super rugby',
        'top 14', 'premiership rugby', 'champions cup', 'challenge cup',
        'all blacks', 'pumas', 'wallabies', 'springboks'
    ]
    
    for kw in rugby_keywords:
        if kw in team1_lower or kw in team2_lower or kw in liga_lower:
            print(f"🚫 Filtrado Rugby: {team1} vs {team2} (liga: {liga})")
            return False
    
    nba_keywords = ['lakers', 'warriors', 'celtics', 'bulls', 'heat', 'bucks']
    mlb_keywords = ['tigers', 'red sox', 'yankees', 'dodgers']
    
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
# BUSCAR EQUIPO
# ============================================

def find_team(query, liga=None):
    if not TEAMS_DB:
        return {'name': query, 'country': 'unknown', 'confidence': 0}
    
    normalized_query = normalize(query)
    sport = get_sport_from_liga(liga) if liga else 'soccer'
    
    best_match = None
    best_score = 0
    
    for slug, team in TEAMS_DB.items():
        team_sport = team.get('sport', 'soccer')
        
        if sport in ['mlb', 'nba'] and team_sport != sport:
            continue
        
        if normalize(team['name']) == normalized_query:
            return {'name': team['name'], 'country': team.get('country', 'usa'), 'confidence': 1.0}
        
        for alias in team.get('aliases', []):
            if normalize(alias) == normalized_query:
                return {'name': team['name'], 'country': team.get('country', 'usa'), 'confidence': 0.95}
        
        if best_score < 0.9:
            if (normalize(team['name']) in normalized_query or 
                normalized_query in normalize(team['name'])):
                score = 0.7
                if score > best_score:
                    best_score = score
                    best_match = {'name': team['name'], 'country': team.get('country', 'usa'), 'confidence': score}
    
    return best_match or {'name': query, 'country': 'unknown', 'confidence': 0}

# ============================================
# OBTENER LOGO
# ============================================

def get_image(liga, team1_country, team2_country):
    # Obtener país de la liga
    country_from_league = LEAGUE_TO_COUNTRY.get(liga, '')
    
    # Si los equipos tienen país y coincide con la liga
    if (team1_country != 'unknown' and team2_country != 'unknown' and 
        team1_country == team2_country and 
        team1_country == country_from_league):
        return COUNTRY_IMAGES.get(team1_country, COUNTRY_IMAGES['default'])
    
    # Si no coincide o no hay país definido, usar default
    return COUNTRY_IMAGES['default']

# ============================================
# CALCULAR HORA DE FINALIZACIÓN
# ============================================

def calcular_hora_fin(hora_inicio_utc, deporte):
    if not hora_inicio_utc:
        return None
    
    try:
        duracion = SPORT_DURATION.get(deporte, SPORT_DURATION['default'])
        hora_limpia = hora_inicio_utc.replace('Z', '')
        
        formatos = [
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M'
        ]
        
        inicio_dt = None
        for fmt in formatos:
            try:
                inicio_dt = datetime.strptime(hora_limpia, fmt)
                break
            except:
                continue
        
        if not inicio_dt:
            return None
        
        fin_dt = inicio_dt + timedelta(minutes=duracion)
        return fin_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    except Exception as e:
        print(f"  ⚠️ Error calculando hora fin: {e}")
        return None

# ============================================
# RESOLVER PARTIDO
# ============================================

# ============================================
# RESOLVER PARTIDO (CORREGIDO - LOGO POR DEPORTE)
# ============================================

def resolve_match(match_text, liga=''):
    """Resuelve el partido usando la nueva función split_match_text"""
    team1_name, team2_name = split_match_text(match_text)
    
    if not team1_name or not team2_name:
        return None
    
    team1 = find_team(team1_name, liga)
    team2 = find_team(team2_name, liga)
    
    if not validate_sport_by_liga(team1['name'], team2['name'], liga):
        return None
    
    sport = get_sport_from_liga(liga)
    
    deportes_con_logo_propio = ['nba', 'mlb', 'nfl', 'ufc', 'wwe', 'box']
    
    if sport in deportes_con_logo_propio:
        image = COUNTRY_IMAGES.get(sport, COUNTRY_IMAGES['default'])
        same_country = False
    else:
        same_country = (team1['country'] != 'unknown' and team1['country'] == team2['country'])
        
        if same_country and team1['country'] != 'unknown':
            image = COUNTRY_IMAGES.get(team1['country'], COUNTRY_IMAGES['default'])
        else:
            # 🔥 CORREGIDO: pasar los países de los equipos
            image = get_image(liga, team1['country'], team2['country'])
    
    return {
        'team1': team1['name'],
        'team2': team2['name'],
        'team1_country': team1['country'],
        'team2_country': team2['country'],
        'same_country': same_country,
        'confidence': round((team1['confidence'] + team2['confidence']) / 2, 2),
        'image': image,
        'sport': sport
    }

# ============================================
# GENERAR matches.json
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
    
    # 🔥 FUNCIÓN PARA GENERAR CLAVE CON TOLERANCIA DE 30 MINUTOS
    def generar_clave_unificacion(equipos, hora_utc):
        """Genera clave de unificación con tolerancia de 30 minutos"""
        if not hora_utc:
            return equipos
        
        try:
            # Limpiar formato de hora
            hora_limpia = hora_utc.replace('Z', '')
            
            # Intentar diferentes formatos
            dt = None
            formatos = [
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M',
                '%Y-%m-%d %H:%M:%S', 
                '%Y-%m-%d %H:%M'
            ]
            
            for fmt in formatos:
                try:
                    dt = datetime.strptime(hora_limpia, fmt)
                    break
                except:
                    continue
            
            if not dt:
                return equipos
            
            # Redondear a los 30 minutos más cercanos
            minutes = dt.minute
            rounded_minutes = (minutes // 30) * 30
            dt_rounded = dt.replace(minute=rounded_minutes, second=0, microsecond=0)
            
            return f"{equipos}|{dt_rounded.isoformat()}"
        except:
            return equipos
    
    unified = {}
    partidos_ignorados = 0
    rugby_filtrados = 0
    
    for item in raw_matches:
        match_text = item.get('equipos') or item.get('match') or item.get('title')
        if not match_text:
            continue
        
        liga = item.get('liga', '').replace(':', '')
        
        if 'UFC' in liga or 'WWE' in liga:
            key = normalizar_evento_especial(match_text, liga)
            resolved = {
                'team1': key.split(' vs ')[0] if ' vs ' in key else key,
                'team2': key.split(' vs ')[1] if ' vs ' in key else '',
                'team1_country': 'unknown',
                'team2_country': 'unknown',
                'same_country': False,
                'confidence': 0.5,
                'image': COUNTRY_IMAGES.get('ufc' if 'UFC' in liga else 'wwe', COUNTRY_IMAGES['default']),
                'sport': 'ufc' if 'UFC' in liga else 'wwe'
            }
        else:
            resolved = resolve_match(match_text, liga)
        
        if not resolved:
            partidos_ignorados += 1
            continue
        
        if 'rugby' in liga.lower() or 'rugby' in match_text.lower():
            rugby_filtrados += 1
            continue
        
        # 🔥 GENERAR CLAVE CON TOLERANCIA DE 30 MINUTOS
        equipos_key = f"{resolved['team1']} vs {resolved['team2']}"
        hora_utc = item.get('hora_utc', '')
        key = generar_clave_unificacion(equipos_key, hora_utc)
        
        hora_fin = calcular_hora_fin(hora_utc, resolved.get('sport', 'soccer'))
        
        if key not in unified:
            unified[key] = {
                'hora_utc': hora_utc,
                'hora_fin_utc': hora_fin,
                'liga': liga,
                'equipos': equipos_key,
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
            existing = unified[key]
            country_from_league = LEAGUE_TO_COUNTRY.get(liga, '')
            team_country = resolved['team1_country'] or resolved['team2_country']
            
            if country_from_league == team_country and team_country != 'unknown':
                existing['liga'] = liga
            elif existing.get('liga'):
                existing_country = LEAGUE_TO_COUNTRY.get(existing['liga'], '')
                if existing_country != team_country and country_from_league == team_country:
                    existing['liga'] = liga
            
            # Usar la hora más temprana
            if hora_utc and (not existing.get('hora_utc') or hora_utc < existing['hora_utc']):
                existing['hora_utc'] = hora_utc
                existing['hora_fin_utc'] = hora_fin
        
        # Agregar canales sin duplicar
        for canal in item.get('canales', []):
            url = canal.get('url', '')
            if not url:
                continue
            
            exists = False
            for existing_canal in unified[key]['canales']:
                if existing_canal.get('url') == url:
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
        print(f"⚠️ Partidos ignorados por formato inválido: {partidos_ignorados}")
    if rugby_filtrados > 0:
        print(f"🚫 Eventos de Rugby filtrados: {rugby_filtrados}")
    
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
