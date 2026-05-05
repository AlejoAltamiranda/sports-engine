import json
import re
from datetime import datetime, timedelta
from collections import defaultdict

# ============================================
# DURACIÓN DE PARTIDOS POR DEPORTE (en minutos)
# ============================================

SPORT_DURATION = {
    'football': 150,
    'nba': 195,
    'mlb': 195,
    'nfl': 195,
    'ufc': 300,
    'wwe': 180,
    'box': 300,
    'nhl': 180,
    'tenis': 180,
    'f1': 180,
    'f2': 180,
    'default': 120
}

CONFIG = {
    'input_file': 'scraper_output.json',
    'output_file': 'matches.json'
}

# ============================================
# CARGAR BASE DE DATOS DE EQUIPOS NBA
# ============================================

def load_nba_teams():
    """Carga la lista de nombres de equipos NBA desde nba_teams.json"""
    nba_teams = []
    try:
        with open('nba_teams.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, list):
                for team in data:
                    if 'name' in team:
                        nba_teams.append(team['name'].lower())
            elif isinstance(data, dict) and 'nba_teams' in data:
                for team in data['nba_teams']:
                    if 'name' in team:
                        nba_teams.append(team['name'].lower())
        print(f"✅ Cargados {len(nba_teams)} equipos NBA")
    except FileNotFoundError:
        print("⚠️ No se encuentra nba_teams.json")
    except Exception as e:
        print(f"⚠️ Error cargando nba_teams.json: {e}")
    return nba_teams

NBA_TEAMS = load_nba_teams()

# ============================================
# EXTRAER SOLO NOMBRES DE EQUIPOS (ignorar prefijos)
# ============================================

def extraer_solo_equipos(texto):
    """
    Extrae solo los nombres de equipos de un texto, ignorando prefijos como:
    "Playoffs Juego #7 – Cleveland Cavaliers vs Toronto Raptors" → "Cleveland Cavaliers vs Toronto Raptors"
    """
    if not texto or ' vs ' not in texto:
        return texto
    
    # Buscar los nombres de equipos NBA conocidos en el texto
    texto_lower = texto.lower()
    equipos_encontrados = []
    
    for team in NBA_TEAMS:
        if team in texto_lower:
            # Capitalizar nombre correctamente
            team_capitalized = ' '.join([word.capitalize() for word in team.split()])
            equipos_encontrados.append(team_capitalized)
    
    # Si encontramos exactamente 2 equipos, reconstruir
    if len(equipos_encontrados) == 2:
        # Ordenar alfabéticamente para unificar
        if equipos_encontrados[0].lower() > equipos_encontrados[1].lower():
            return f"{equipos_encontrados[1]} vs {equipos_encontrados[0]}"
        return f"{equipos_encontrados[0]} vs {equipos_encontrados[1]}"
    
    # Si no se encuentran equipos NBA, intentar extraer lo que está después del guion o separador
    separadores = [' – ', ' - ', ': ']
    for sep in separadores:
        if sep in texto:
            # Tomar la parte después del separador
            texto = texto.split(sep, 1)[1]
            break
    
    return texto.strip()

# ============================================
# LIMPIAR NOMBRE DE LIGA
# ============================================

def limpiar_nombre_liga(liga):
    """
    Limpia el nombre de la liga eliminando todo lo que viene después de:
    - , (coma)
    - – (guion largo)
    - - (guion normal)
    """
    if not liga:
        return liga
    
    separadores = [' – ', ' - ', ', ']
    
    for sep in separadores:
        if sep in liga:
            liga = liga.split(sep)[0]
            break
    
    return liga.strip()

# ============================================
# ASIGNAR LOGO O EMOJI SEGÚN LIGA
# ============================================

def asignar_logo(liga):
    """Asigna el logo correspondiente según la liga. Si no hay logo, retorna None."""
    liga_lower = liga.lower()
    
    if 'tenis' in liga_lower or 'tennis' in liga_lower:
        return 'img/tenis.png'
    if 'nba' in liga_lower or 'wnba' in liga_lower:
        return 'img/nba.png'
    if 'mlb' in liga_lower:
        return 'img/mlb.png'
    if 'nfl' in liga_lower:
        return 'img/nfl.png'
    if 'nhl' in liga_lower:
        return 'img/nhl.png'
    if 'wwe' in liga_lower:
        return 'img/wwe.png'
    if 'ufc' in liga_lower:
        return 'img/ufc.png'
    if 'box' in liga_lower or 'boxeo' in liga_lower:
        return 'img/box.png'
    if 'f1' in liga_lower or 'formula 1' in liga_lower:
        return 'img/formula1.png'
    if 'f2' in liga_lower or 'formula 2' in liga_lower:
        return 'img/formula1.png'
    
    return None  # No hay logo, se usará emoji

def asignar_emoji(liga):
    """Asigna un emoji correspondiente según la liga."""
    liga_lower = liga.lower()
    
    if 'tenis' in liga_lower or 'tennis' in liga_lower:
        return '🎾'
    if 'nba' in liga_lower or 'wnba' in liga_lower:
        return '🏀'
    if 'mlb' in liga_lower:
        return '⚾'
    if 'golf' in liga_lower:
        return '⛳'
    if 'nfl' in liga_lower:
        return '🏈'
    if 'nhl' in liga_lower:
        return '🏒'
    if 'wwe' in liga_lower:
        return '🤼'
    if 'ufc' in liga_lower:
        return '🥋'
    if 'box' in liga_lower or 'boxeo' in liga_lower:
        return '🥊'
    if 'f1' in liga_lower or 'formula 1' in liga_lower:
        return '🏎️'
    if 'f2' in liga_lower or 'formula 2' in liga_lower:
        return '🏎️'
    
    return '⚽'  # Emoji por defecto para fútbol

# ============================================
# DETECCIÓN DE DEPORTE
# ============================================

def get_sport_from_liga(liga):
    liga_lower = liga.lower()
    
    if 'tenis' in liga_lower or 'tennis' in liga_lower:
        return 'tenis'
    if 'nba' in liga_lower or 'wnba' in liga_lower:
        return 'nba'
    if 'mlb' in liga_lower:
        return 'mlb'
    if 'nfl' in liga_lower:
        return 'nfl'
    if 'nhl' in liga_lower:
        return 'nhl'
    if 'wwe' in liga_lower:
        return 'wwe'
    if 'f1' in liga_lower or 'formula 1' in liga_lower:
        return 'f1'
    if 'f2' in liga_lower or 'formula 2' in liga_lower:
        return 'f2'
    if 'ufc' in liga_lower:
        return 'ufc'
    if 'box' in liga_lower or 'boxeo' in liga_lower:
        return 'box'
    return 'football'

# ============================================
# CALCULAR HORA FIN
# ============================================

def calcular_hora_fin(hora_inicio_utc, deporte):
    if not hora_inicio_utc:
        return None
    
    try:
        duracion = SPORT_DURATION.get(deporte, SPORT_DURATION['default'])
        hora_limpia = hora_inicio_utc.replace('Z', '')
        
        formatos = ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M']
        
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
        return None

# ============================================
# NORMALIZAR F1/F2
# ============================================

def normalizar_texto_f1_f2(texto, sport):
    """Normaliza textos de F1 y F2 para unificar eventos iguales en diferentes idiomas"""
    if not texto:
        return texto
    
    es_f1 = sport == 'f1'
    es_f2 = sport == 'f2'
    
    if not (es_f1 or es_f2):
        return texto
    
    texto_lower = texto.lower()
    
    # Nombres de Grandes Premios
    gp_nombres = {
        'miami': 'Miami', 'mónaco': 'Monaco', 'monaco': 'Monaco',
        'singapur': 'Singapore', 'singapore': 'Singapore',
        'japón': 'Japan', 'japon': 'Japan', 'japan': 'Japan', 'suzuka': 'Japan',
        'españa': 'Spain', 'spain': 'Spain', 'barcelona': 'Spain',
        'italia': 'Italy', 'italy': 'Italy', 'monza': 'Italy',
        'inglaterra': 'Great Britain', 'england': 'Great Britain', 'silverstone': 'Great Britain',
        'austria': 'Austria', 'spielberg': 'Austria',
        'hungría': 'Hungary', 'hungary': 'Hungary', 'hungaroring': 'Hungary',
        'bélgica': 'Belgium', 'belgium': 'Belgium', 'spa': 'Belgium',
        'francia': 'France', 'france': 'France', 'paul ricard': 'France',
        'holanda': 'Netherlands', 'netherlands': 'Netherlands', 'zandvoort': 'Netherlands',
        'méxico': 'Mexico', 'mexico': 'Mexico', 'ciudad de méxico': 'Mexico',
        'brasil': 'Brazil', 'brazil': 'Brazil', 'interlagos': 'Brazil',
        'bahrein': 'Bahrain', 'bahrain': 'Bahrain',
        'qatar': 'Qatar',
        'abu dhabi': 'Abu Dhabi',
        'yeda': 'Saudi Arabia', 'jeddah': 'Saudi Arabia',
        'austin': 'USA', 'usa': 'USA', 'estados unidos': 'USA',
        'canadá': 'Canada', 'canada': 'Canada', 'montreal': 'Canada',
        'china': 'China', 'shanghai': 'China',
        'australia': 'Australia', 'melbourne': 'Australia',
        'azerbaijan': 'Azerbaijan', 'baku': 'Azerbaijan',
        'turkey': 'Turkey', 'istanbul': 'Turkey'
    }
    
    # Detectar nombre del GP
    gp_nombre = None
    for key, value in gp_nombres.items():
        if key in texto_lower:
            gp_nombre = value
            break
    
    if not gp_nombre:
        gp_match = re.search(r'gp\s+([a-z]+)', texto_lower)
        if gp_match:
            gp_nombre = gp_match.group(1).capitalize()
    
    if not gp_nombre and 'grand prix' in texto_lower:
        match = re.search(r'([a-z\s]+?)\s+grand prix', texto_lower)
        if match:
            gp_nombre = match.group(1).strip().title()
    
    if not gp_nombre:
        gp_nombre = 'GP'
    
    # Detectar sesión
    session = None
    texto_lower_clean = re.sub(r'[^\w\s]', '', texto_lower)
    
    if any(p in texto_lower_clean for p in ['qualifying', 'clasificacion', 'clasificación', 'qualy', 'qual']):
        session = 'Qualifying'
    elif 'sprint' in texto_lower_clean:
        session = 'Sprint'
    elif any(p in texto_lower_clean for p in ['practice', 'práctica', 'practica', 'fp']):
        if '3' in texto_lower_clean or 'fp3' in texto_lower_clean:
            session = 'Practice 3'
        elif '2' in texto_lower_clean or 'fp2' in texto_lower_clean:
            session = 'Practice 2'
        else:
            session = 'Practice 1'
    elif any(p in texto_lower_clean for p in ['grand prix', 'race', 'carrera']):
        session = 'Grand Prix'
    else:
        session = 'Grand Prix'
    
    categoria = 'F1' if es_f1 else 'F2'
    return f"{categoria} {gp_nombre} - {session}"

# ============================================
# ORDENAR EQUIPOS ALFABÉTICAMENTE
# ============================================

def ordenar_equipos(equipos_texto):
    """Ordena los equipos alfabéticamente para unificar partidos"""
    if not equipos_texto or ' vs ' not in equipos_texto:
        return equipos_texto
    
    partes = equipos_texto.split(' vs ')
    if len(partes) != 2:
        return equipos_texto
    
    team1, team2 = partes[0].strip(), partes[1].strip()
    
    if team1.lower() > team2.lower():
        return f"{team2} vs {team1}"
    return equipos_texto

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
        return
    
    print(f"📊 Procesando {len(raw_matches)} eventos...")
    
    def generar_clave(liga, equipos, hora_utc, sport=None):
        # Para F1, F2 y NBA, ignorar la hora y unificar SOLO por liga + equipos
        if sport in ['f1', 'f2', 'nba']:
            return f"{liga}|{equipos}"
        
        # Para el resto de deportes, usar hora redondeada
        if not hora_utc:
            return f"{liga}|{equipos}"
        
        try:
            # Normalizar hora: si no tiene segundos, agregar :00
            hora_limpia = hora_utc.replace('Z', '')
            if len(hora_limpia) == 16:  # Formato: YYYY-MM-DDTHH:MM
                hora_limpia = hora_limpia + ':00'
            
            dt = datetime.strptime(hora_limpia, '%Y-%m-%dT%H:%M:%S')
            hora_base = (dt.hour // 3) * 3
            dt_rounded = dt.replace(hour=hora_base, minute=0, second=0)
            return f"{liga}|{equipos}|{dt_rounded.isoformat()}"
        except:
            return f"{liga}|{equipos}"
    
    unified = {}
    
    for item in raw_matches:
        match_text = item.get('equipos')
        if not match_text:
            continue
        
        liga = item.get('liga', '').replace(':', '')
        liga = limpiar_nombre_liga(liga)
        
        sport_actual = get_sport_from_liga(liga)
        
        # 🔥 PARA NBA: Extraer solo los nombres de equipos (ignorar prefijos como "Playoffs Juego #7")
        if sport_actual == 'nba':
            match_text = extraer_solo_equipos(match_text)
        
        equipos_ordenados = ordenar_equipos(match_text)
        
        # 🔥 NUEVA LÓGICA: Priorizar logo del scraper, si no existe o es default, usar emoji
        logo_original = item.get('logo', '')
        
        # Si el logo original es default o no existe, usar emoji
        if not logo_original or logo_original == 'img/default.png':
            logo = asignar_emoji(liga)
        else:
            logo = logo_original
        
        if sport_actual in ['f1', 'f2']:
            equipos_ordenados = normalizar_texto_f1_f2(equipos_ordenados, sport_actual)
        
        hora_utc = item.get('hora_utc', '')
        key = generar_clave(liga, equipos_ordenados, hora_utc, sport_actual)
        hora_fin = calcular_hora_fin(hora_utc, sport_actual)
        
        if key not in unified:
            unified[key] = {
                'hora_utc': hora_utc,
                'hora_fin_utc': hora_fin,
                'liga': liga,
                'equipos': equipos_ordenados,
                'logo': logo,
                'sport': sport_actual,
                'canales': []
            }
        
        for canal in item.get('canales', []):
            url = canal.get('url', '')
            if not url:
                continue
            
            exists = any(c.get('url') == url for c in unified[key]['canales'])
            if not exists:
                unified[key]['canales'].append({
                    'nombre': canal.get('nombre', 'Canal'),
                    'url': url,
                    'calidad': canal.get('calidad', 'HD')
                })
        
        if hora_utc and (not unified[key]['hora_utc'] or hora_utc < unified[key]['hora_utc']):
            unified[key]['hora_utc'] = hora_utc
            unified[key]['hora_fin_utc'] = calcular_hora_fin(hora_utc, sport_actual)
    
    matches_list = list(unified.values())
    matches_list.sort(key=lambda x: x.get('hora_utc', ''))
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(matches_list, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Generado {output_file} con {len(matches_list)} partidos")
    
    deportes = defaultdict(int)
    for m in matches_list:
        deportes[m.get('sport', 'football')] += 1
    
    print(f"📊 Desglose por deporte:")
    for deporte, count in sorted(deportes.items(), key=lambda x: x[1], reverse=True):
        print(f"   - {deporte}: {count}")
    
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
  python engine.py batch → Generar matches.json
        """)
    elif sys.argv[1] == 'batch':
        generate_matches_json()
    else:
        print("Comando no reconocido")
