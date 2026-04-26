import json
import urllib.request
import re
from datetime import datetime, timedelta
from collections import defaultdict

# ============================================
# CONFIGURACIÓN GLOBAL
# ============================================
SOURCES = {
    'pltvhd': 'https://pltvhd.com/diaries.json',
    'github': 'https://raw.githubusercontent.com/AlejoAltamiranda/stream/refs/heads/main/events.json'
}
OUTPUT_FILE = 'scraper_output.json'

# ============================================
# DETECCIÓN Y CONVERSIÓN DE HORAS
# ============================================

def detectar_tipo_hora(hora_str):
    """
    Detecta si una hora es UTC o Local
    UTC: termina con Z o tiene formato ISO con Z
    Local: formato ISO sin Z, o solo hora (ej: "16:00")
    """
    if not hora_str:
        return 'desconocido'
    
    # Si tiene Z al final, es UTC
    if hora_str.endswith('Z'):
        return 'utc'
    
    # Si tiene T pero no Z, es local
    if 'T' in hora_str and not hora_str.endswith('Z'):
        return 'local'
    
    # Si es solo hora (ej: "16:00" o "14:30"), es local
    if ':' in hora_str and len(hora_str) <= 8:
        return 'local'
    
    return 'desconocido'

def convertir_local_a_utc(hora_str, fecha_referencia=None):
    """Convierte hora LOCAL de Colombia (UTC-5) a UTC"""
    if not hora_str:
        return None
    
    try:
        # Limpiar formato
        if 'T' in hora_str:
            hora_limpia = hora_str.replace('Z', '').replace('T', ' ')
        else:
            hora_limpia = hora_str
        
        # Añadir fecha si solo es hora
        if ':' in hora_limpia and len(hora_limpia) <= 8:
            if fecha_referencia:
                fecha_str = fecha_referencia.strftime('%Y-%m-%d')
            else:
                fecha_str = datetime.now().strftime('%Y-%m-%d')
            hora_completa = f"{fecha_str} {hora_limpia}"
        else:
            hora_completa = hora_limpia
        
        # Intentar diferentes formatos
        formatos = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M'
        ]
        
        local_dt = None
        for fmt in formatos:
            try:
                local_dt = datetime.strptime(hora_completa, fmt)
                break
            except:
                continue
        
        if not local_dt:
            return hora_str
        
        # Colombia UTC-5 → sumar 5 horas
        utc_dt = local_dt + timedelta(hours=5)
        return utc_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    except Exception as e:
        print(f"  ⚠️ Error convirtiendo {hora_str}: {e}")
        return hora_str

def formatear_utc(hora_str):
    """Formatea una hora que ya está en UTC"""
    if not hora_str:
        return None
    
    try:
        # Si tiene espacio, reemplazar por T
        if ' ' in hora_str:
            hora_str = hora_str.replace(' ', 'T')
        
        # Si no tiene Z, agregarla
        if not hora_str.endswith('Z'):
            hora_str = hora_str + 'Z'
        
        return hora_str
    except:
        return hora_str

def procesar_hora_segun_fuente(hora_str, fuente, fecha_ref=None):
    """
    Procesa la hora según la fuente con detección automática
    - github: siempre UTC → formatear
    - pltvhd: detectar si es UTC o LOCAL → actuar según corresponda
    """
    if not hora_str:
        return None
    
    # Fuentes que siempre son UTC (no necesitan detección)
    if fuente == 'github':
        return formatear_utc(hora_str)
    
    # Fuentes que pueden ser UTC o LOCAL (detección automática)
    if fuente == 'pltvhd':
        tipo = detectar_tipo_hora(hora_str)
        if tipo == 'utc':
            print(f"  🔍 {fuente}: UTC detectado → {hora_str}")
            return formatear_utc(hora_str)
        else:
            print(f"  🔍 {fuente}: LOCAL detectado → {hora_str} (convirtiendo a UTC)")
            return convertir_local_a_utc(hora_str, fecha_ref)
    
    return hora_str

# ============================================
# MAPEO DE LIGAS A PAÍSES (para logos)
# ============================================
TOURNAMENT_TO_COUNTRY = {
    'La Liga': 'spain', 'LaLiga': 'spain', 'LaLiga 2:': 'spain',
    'Copa Alemania': 'germany', 'DFB Pokal': 'germany',
    'Eredivisie': 'netherlands',
    'Copa de Portugal': 'portugal', 'Taça de Portugal': 'portugal', 'Primeira Liga': 'portugal',
    'Liga 1': 'peru', 'Primera Division': 'peru',
    'Copa do Brasil': 'brazil', 'Brasileirão': 'brazil',
    'Liga Profesional': 'argentina', 'Primera División': 'argentina', 'Futbol Argentino': 'argentina',
    'Liga BetPlay': 'colombia', 'Liga Betplay': 'colombia', 'Primera A': 'colombia',
    'Liga de Primera': 'chile', 'Futbol Chileno': 'chile',
    'Liga Pro': 'ecuador',
    'Turkish Cup': 'turkey',
    'Pro League': 'arabia',
    'Primera División Uruguay': 'uruguay', 'Liga Uruguaya': 'uruguay','Uruguayan Primera División': 'uruguay',
    'Torneo Apertura Uruguay': 'uruguay','Torneo Clausura Uruguay': 'uruguay',
    'Champions League': 'champions', 'Champions': 'champions', 'UEFA Champions League': 'champions',
    'mls': 'estados unidos', 'MLS': 'usa', 'mls': 'usa',
    'MLB': 'mlb',
    'NBA': 'nba',
    'WWE': 'wwe',
    'UFC': 'ufc',
    'Boxing': 'box',
    'NFL': 'nfl',
    'NHL': 'nhl',

}

COUNTRY_LOGO = {
    'spain': 'img/es.png', 'germany': 'img/de.png', 'netherlands': 'img/nl.png',
    'portugal': 'img/pt.png', 'peru': 'img/pe.png', 'brazil': 'img/br.png',
    'argentina': 'img/ar.png', 'colombia': 'img/col.png', 'chile': 'img/cl.png',
    'ecuador': 'img/ec.png', 'turkey': 'img/tr.png', 'usa': 'img/usa.png', 
    'arabia': 'img/sa.png', 'paraguay': 'img/py.png', 'uruguay': 'img/uy.png',
    'mexico': 'img/mx.png', 'nba': 'img/nba.png', 'mlb': 'img/mlb.png', 'wwe': 'img/wwe.png', 
    'champions':'img/champions.png', 'ufc': 'img/ufc.png', 'box': 'img/box.png', 
    'nhl': 'img/nhl.png', 'nfl': 'img/nfl.png', 'default': 'img/default.png'
}

# ============================================
# FUNCIONES COMUNES
# ============================================

def get_logo(liga, country_from_source=None):
    if country_from_source and country_from_source.lower() in COUNTRY_LOGO:
        return COUNTRY_LOGO[country_from_source.lower()]
    country = TOURNAMENT_TO_COUNTRY.get(liga, 'default')
    return COUNTRY_LOGO.get(country, COUNTRY_LOGO['default'])

def extract_channel_name(url):
    match = re.search(r'stream=([^&]+)', url)
    if match:
        name = match.group(1).replace('dsports', 'Dsports').replace('espn', 'ESPN')
        name = name.replace('premiere', 'Premiere').replace('fanatiz', 'Fanatiz')
        name = name.replace('disney', 'Disney+').replace('winplus', 'Win Sports+')
        name = re.sub(r'([a-z])(\d+)', r'\1 \2', name)
        return name.title()
    return 'Canal'

def fetch_json(url):
    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            return json.loads(response.read().decode('utf-8'))
    except Exception as e:
        print(f"  ⚠️ Error en {url}: {e}")
        return None

# ============================================
# PROCESADORES DE CADA FUENTE
# ============================================

def process_github_source(data):
    matches = []
    if not data:
        return matches
    
    # Nuevo formato: array directo
    if isinstance(data, list):
        for event in data:
            if 'title' not in event:
                continue
            equipos = event['title']
            liga = event.get('category', 'MLB')
            hora_raw = event.get('time', '')
            # Convertir "2026-04-24 22:40 UTC" a ISO
            if hora_raw:
                hora_utc = hora_raw.replace(' UTC', 'Z').replace(' ', 'T')
            else:
                hora_utc = ''
            
            logo = get_logo(liga)
            matches.append({
                'hora_utc': hora_utc,
                'logo': logo,
                'liga': liga,
                'equipos': equipos,
                'status': 'pronto',
                'canales': [{'nombre': event.get('channel', 'Canal'), 'url': event.get('link', ''), 'calidad': 'HD'}],
                'fuente': 'github'
            })
        return matches
    
    # Formato antiguo: con objeto 'events'
    if 'events' in data:
        for event in data['events']:
            if 'title' not in event:
                continue
            equipos = event['title']
            liga = event.get('category', 'MLB')
            hora_raw = event.get('time_utc', '')
            hora_utc = procesar_hora_segun_fuente(hora_raw, 'github')
            logo = get_logo(liga)
            matches.append({
                'hora_utc': hora_utc,
                'logo': logo,
                'liga': liga,
                'equipos': equipos,
                'status': 'pronto',
                'canales': [{'nombre': event.get('channel', 'Canal'), 'url': event.get('link', ''), 'calidad': 'HD'}],
                'fuente': 'github'
            })
    
    return matches

def process_pltvhd_source(data):
    """Procesador para PLTVHD con filtro que excluye partidos de Rugby"""
    matches = []
    if not data:
        return matches
    
    # Palabras clave para detectar Rugby (y otros deportes no deseados)
    PALABRAS_RUGBY = [
        'rugby', 'rugbi', 'six nations', 'seis naciones', 'super rugby',
        'top 14', 'premiership rugby', 'champions cup', 'challenge cup',
        'uruguay rugby', 'chile rugby', 'argentina rugby', 'brasil rugby',
        'rugby championship', 'the rugby championship', 'test match'
    ]
    
    today = datetime.now()
    
    # La estructura tiene un objeto 'data' que contiene la lista de eventos
    events_list = data.get('data', []) if isinstance(data, dict) else []
    eventos_filtrados = 0
    
    for item in events_list:
        # Extraer attributes
        attrs = item.get('attributes', {})
        
        # Obtener la descripción del evento (ej: "Primera División: Oriente Petrolero vs Real Potosí")
        diary_description = attrs.get('diary_description', '')
        
        # 🔥 FILTRAR RUGBY: Convertir a minúsculas y verificar palabras clave
        desc_lower = diary_description.lower()
        es_rugby = False
        for palabra in PALABRAS_RUGBY:
            if palabra in desc_lower:
                es_rugby = True
                eventos_filtrados += 1
                print(f"  🏉 Filtrado Rugby: {diary_description[:60]}...")
                break
        
        if es_rugby:
            continue  # Saltar este evento
        
        # Extraer liga y equipos de la descripción
        liga = ''
        equipos = ''
        if ': ' in diary_description:
            liga, equipos = diary_description.split(': ', 1)
        else:
            equipos = diary_description
            liga = 'Fútbol'
        
        # Obtener fecha y hora
        date_diary = attrs.get('date_diary', '')  # Formato: "2026-04-26"
        diary_hour = attrs.get('diary_hour', '')  # Formato: "18:30:00"
        
        # Construir hora completa en formato local (Colombia UTC-5)
        if date_diary and diary_hour:
            hora_local = f"{date_diary} {diary_hour[:5]}"  # Tomar solo HH:MM
        else:
            hora_local = ''
        
        # Procesar hora (asumimos que es hora LOCAL de Colombia)
        hora_utc = procesar_hora_segun_fuente(hora_local, 'pltvhd', today)
        
        # Obtener los canales/embeds
        canales = []
        embeds = attrs.get('embeds', {})
        embeds_data = embeds.get('data', []) if isinstance(embeds, dict) else []
        
        for embed in embeds_data:
            embed_attrs = embed.get('attributes', {})
            embed_name = embed_attrs.get('embed_name', 'Canal')
            embed_iframe = embed_attrs.get('embed_iframe', '')
            
            # Construir URL completa si es necesario
            if embed_iframe and embed_iframe.startswith('/'):
                embed_url = f"https://pltvhd.com{embed_iframe}"
            else:
                embed_url = embed_iframe
            
            canales.append({
                'nombre': embed_name,
                'url': embed_url,
                'calidad': 'HD'
            })
        
        # Si no hay canales en embeds, revisar channels
        if not canales:
            channels = attrs.get('channels', {})
            channels_data = channels.get('data', []) if isinstance(channels, dict) else []
            for channel in channels_data:
                channel_attrs = channel.get('attributes', {})
                channel_name = channel_attrs.get('channel_name', 'Canal')
                channel_url = channel_attrs.get('channel_url', '')
                canales.append({
                    'nombre': channel_name,
                    'url': channel_url,
                    'calidad': 'HD'
                })
        
        # Si todavía no hay canales, agregar un placeholder
        if not canales:
            canales = [{'nombre': 'Canal 1', 'url': '#', 'calidad': 'HD'}]
        
        logo = get_logo(liga)
        
        matches.append({
            'hora_utc': hora_utc,
            'logo': logo,
            'liga': liga,
            'equipos': equipos,
            'status': 'pronto',
            'canales': canales,
            'fuente': 'pltvhd'
        })
    
    if eventos_filtrados > 0:
        print(f"  🏉 TOTAL: {eventos_filtrados} eventos de Rugby filtrados y descartados")
    
    return matches

# ============================================
# ORQUESTADOR PRINCIPAL
# ============================================

def run_scraper():
    print('\n🏆 SCRAPER MULTIFUENTE (TODO a UTC con detección)\n')
    all_matches = []
    processors = [
        ('PLTVHD', SOURCES['pltvhd'], process_pltvhd_source),
        ('GitHub (MLB)', SOURCES['github'], process_github_source)
    ]
    
    for name, url, processor in processors:
        print(f'📡 {name}...')
        data = fetch_json(url)
        if data:
            matches = processor(data)
            print(f'   ✅ {len(matches)} partidos (después de filtrar Rugby)')
            all_matches.extend(matches)
        else:
            print(f'   ❌ Sin datos')
    
    # UNIFICAR PARTIDOS DUPLICADOS
    unique = {}
    for m in all_matches:
        if not m.get('hora_utc'):
            continue
        key = f"{m['equipos']}|{m['hora_utc'][:16]}"
        if key not in unique:
            unique[key] = m
        else:
            existing = unique[key]
            existing['canales'].extend(m['canales'])
            if existing['logo'] == COUNTRY_LOGO['default'] and m['logo'] != COUNTRY_LOGO['default']:
                existing['logo'] = m['logo']
    
    final_matches = list(unique.values())
    final_matches.sort(key=lambda x: x.get('hora_utc', ''))
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_matches, f, indent=2, ensure_ascii=False)
    
    print(f'\n💾 Guardados {len(final_matches)} partidos únicos en {OUTPUT_FILE}')
    
    ligas = defaultdict(int)
    for m in final_matches:
        ligas[m['liga']] += 1
    print('\n📋 Resumen por liga:')
    for liga, count in sorted(ligas.items(), key=lambda x: x[1], reverse=True)[:15]:
        print(f'   - {liga}: {count} partidos')
    if len(ligas) > 15:
        print(f'   ... y {len(ligas)-15} ligas más')

if __name__ == '__main__':
    run_scraper()
