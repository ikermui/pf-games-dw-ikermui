from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import subprocess
import os
import ast
import tempfile
import time

# ============================================================
# CONFIGURACIÓN
# ============================================================

CSV_GAMES      = "/home/ikermuinos/Descargas/games2cleaned_10k_new.csv"
CSV_PLATFORM   = "/home/ikermuinos/Descargas/platform.csv"
CSV_DEVELOPERS = "/home/ikermuinos/Descargas/developers.csv"
CSV_PUBLISHERS = "/home/ikermuinos/Descargas/publishers.csv"
CSV_GAME_PLAT  = "/home/ikermuinos/Descargas/game_platforms.csv"

TMP_PATH       = "/tmp/games_etl_v2"
HIVE_DB        = "games_dw_v2"
HIVE_URL       = "jdbc:hive2://localhost:10000"
HADOOP_USER    = "ikermuinos"

# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def setup():
    os.makedirs(TMP_PATH, exist_ok=True)
    print(f"Directorio temporal creado: {TMP_PATH}")


def run_hive(query):
    """Ejecuta una query HiveQL via beeline."""
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.sql', delete=False, dir='/tmp'
    ) as f:
        f.write(query + "\n")
        tmp_path = f.name

    env = os.environ.copy()
    env['HADOOP_USER_NAME'] = HADOOP_USER

    try:
        result = subprocess.run(
            ["beeline", "-u", HIVE_URL, "-f", tmp_path],
            capture_output=True, text=True, env=env
        )
        print(f"RETURNCODE: {result.returncode}")
        print(f"STDERR: {result.stderr[-300:] if len(result.stderr) > 300 else result.stderr}")

        if result.returncode != 0:
            raise Exception(
                f"Beeline fallo con codigo {result.returncode}\n"
                f"STDOUT: {result.stdout}\n"
                f"STDERR: {result.stderr}"
            )
        return result.stdout
    finally:
        os.unlink(tmp_path)


def load_tsv_to_orc(tabla, filepath, columnas):
    """
    Carga datos en tabla ORC via staging TEXTFILE:
    1. Sube TSV a HDFS
    2. Crea tabla staging TEXTFILE
    3. LOAD DATA desde HDFS a staging
    4. INSERT INTO tabla ORC desde staging
    5. Borra staging
    """
    staging   = f"staging_{tabla}"
    hdfs_path = f"/tmp/games_etl_v2/{os.path.basename(filepath)}"
    cols_def  = ", ".join(f"`{c}` STRING" for c in columnas)

    # Subir TSV a HDFS
    print(f"[{tabla}] Subiendo TSV a HDFS...")
    env = os.environ.copy()
    env['HADOOP_USER_NAME'] = HADOOP_USER
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/tmp/games_etl_v2"], env=env)
    subprocess.run(["hdfs", "dfs", "-put", "-f", filepath, hdfs_path], env=env)
    print(f"[{tabla}] TSV subido: {hdfs_path}")

    print(f"[{tabla}] Paso 1: borrando staging...")
    run_hive(f"USE {HIVE_DB}; DROP TABLE IF EXISTS {staging};")

    print(f"[{tabla}] Paso 2: creando staging...")
    run_hive(f"""
        USE {HIVE_DB};
        CREATE TABLE {staging} ({cols_def})
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\\t'
        STORED AS TEXTFILE;
    """)

    print(f"[{tabla}] Paso 3: cargando TSV en staging...")
    run_hive(f"USE {HIVE_DB}; LOAD DATA INPATH '{hdfs_path}' OVERWRITE INTO TABLE {staging};")

    print(f"[{tabla}] Esperando 5s...")
    time.sleep(5)

    print(f"[{tabla}] Paso 4: INSERT staging → ORC...")
    run_hive(f"""
        USE {HIVE_DB};
        INSERT INTO TABLE {tabla} SELECT * FROM {staging};
    """)
    print(f"[{tabla}] Paso 4: OK")

    print(f"[{tabla}] Paso 5: borrando staging...")
    run_hive(f"USE {HIVE_DB}; DROP TABLE IF EXISTS {staging};")
    print(f"[{tabla}] Carga completada")


def write_tsv(filepath, rows, columns):
    """Escribe un fichero TSV con tabulador como separador."""
    with open(filepath, 'w', encoding='utf-8', newline='') as f:
        for row in rows:
            line = "\t".join(
                str(row.get(c, '')).replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                for c in columns
            )
            f.write(line + "\n")


def parse_array_field(value):
    """Convierte un campo tipo "['Action', 'Free To Play']" en lista Python."""
    if not value or str(value).strip() in ('[]', '', 'nan'):
        return []
    try:
        result = ast.literal_eval(str(value).strip())
        if isinstance(result, list):
            return [str(x).strip() for x in result if str(x).strip()]
        return []
    except Exception:
        return []


def get_price_range(price):
    """Calcula el rango de precio."""
    try:
        p = float(price)
    except Exception:
        return 'Desconocido'
    if p == 0:
        return 'Gratuito'
    elif p <= 5:
        return '0-5euro'
    elif p <= 20:
        return '5-20euro'
    elif p <= 60:
        return '20-60euro'
    else:
        return '+60euro'


def parse_owners(owners_str):
    """Convierte "50000000 - 100000000" en (50000000, 100000000)."""
    try:
        parts = str(owners_str).split(' - ')
        return int(parts[0].strip()), int(parts[1].strip())
    except Exception:
        return 0, 0


def get_quarter(month):
    """Devuelve el trimestre a partir del mes."""
    try:
        m = int(month)
        return (m - 1) // 3 + 1
    except Exception:
        return 0


# ============================================================
# TAREAS ETL
# ============================================================

def cargar_dim_platform():
    setup()
    import pandas as pd
    df   = pd.read_csv(CSV_PLATFORM)
    cols = ['platform_id', 'platform_name', 'windows', 'mac',
            'linux', 'launch_year', 'market_share_pct']
    rows = []
    for _, row in df.iterrows():
        rows.append({
            'platform_id':      int(row['platform_id']),
            'platform_name':    str(row['platform_name']),
            'windows':          str(str(row['windows']).strip().lower() == 'true').upper(),
            'mac':              str(str(row['mac']).strip().lower() == 'true').upper(),
            'linux':            str(str(row['linux']).strip().lower() == 'true').upper(),
            'launch_year':      int(row['launch_year']),
            'market_share_pct': float(row['market_share_pct'])
        })
    filepath = f"{TMP_PATH}/dim_platform.tsv"
    write_tsv(filepath, rows, cols)
    load_tsv_to_orc("dim_platform", filepath, cols)
    print(f"dim_platform cargada: {len(rows)} registros")


def cargar_dim_developer():
    setup()
    import pandas as pd
    df   = pd.read_csv(CSV_DEVELOPERS)
    cols = ['developer_id', 'name', 'country', 'founding_year',
            'size', 'is_closed', 'continent']
    rows = []
    for _, row in df.iterrows():
        rows.append({
            'developer_id':  int(row['developer_id']),
            'name':          str(row['name']).replace('\t', ' ').replace('\n', ' '),
            'country':       str(row['country']),
            'founding_year': int(row['founding_year']),
            'size':          str(row['size']),
            'is_closed':     str(str(row['is_closed']).strip().lower() == 'true').upper(),
            'continent':     str(row['continent'])
        })
    filepath = f"{TMP_PATH}/dim_developer.tsv"
    write_tsv(filepath, rows, cols)
    load_tsv_to_orc("dim_developer", filepath, cols)
    print(f"dim_developer cargada: {len(rows)} registros")


def cargar_dim_publisher():
    setup()
    import pandas as pd
    df   = pd.read_csv(CSV_PUBLISHERS)
    cols = ['publisher_id', 'name', 'country', 'founding_year',
            'size', 'is_closed', 'continent']
    rows = []
    for _, row in df.iterrows():
        rows.append({
            'publisher_id':  int(row['publisher_id']),
            'name':          str(row['name']).replace('\t', ' ').replace('\n', ' '),
            'country':       str(row['country']),
            'founding_year': int(row['founding_year']),
            'size':          str(row['size']),
            'is_closed':     str(str(row['is_closed']).strip().lower() == 'true').upper(),
            'continent':     str(row['continent'])
        })
    filepath = f"{TMP_PATH}/dim_publisher.tsv"
    write_tsv(filepath, rows, cols)
    load_tsv_to_orc("dim_publisher", filepath, cols)
    print(f"dim_publisher cargada: {len(rows)} registros")


def cargar_dim_date():
    setup()
    import pandas as pd
    from datetime import datetime as dt
    df   = pd.read_csv(CSV_GAMES, low_memory=False)
    cols = ['date_id', 'release_date', 'release_year',
            'release_month', 'release_quarter']
    fechas_unicas = {}
    date_id = 1
    for fecha_str in df['release_date'].dropna().unique():
        fecha_str = str(fecha_str).strip()
        if fecha_str in fechas_unicas:
            continue
        try:
            fecha = dt.strptime(fecha_str, '%Y-%m-%d')
            fechas_unicas[fecha_str] = {
                'date_id':         date_id,
                'release_date':    fecha_str,
                'release_year':    fecha.year,
                'release_month':   fecha.month,
                'release_quarter': get_quarter(fecha.month)
            }
            date_id += 1
        except Exception:
            continue
    filepath = f"{TMP_PATH}/dim_date.tsv"
    write_tsv(filepath, list(fechas_unicas.values()), cols)
    load_tsv_to_orc("dim_date", filepath, cols)
    print(f"dim_date cargada: {len(fechas_unicas)} registros")


def cargar_dim_game():
    setup()
    import pandas as pd
    df   = pd.read_csv(CSV_GAMES, low_memory=False)
    cols = ['app_id', 'name', 'required_age', 'is_free', 'dlc_count',
            'game_engine', 'translated_languages', 'dubbed_languages',
            'genres', 'categories']
    rows = []
    for _, row in df.iterrows():
        try:
            price = float(row['price'])
        except Exception:
            price = 0.0

        is_free_raw = str(row.get('is_free', '')).strip().lower()
        if is_free_raw in ('true', '1', 'free'):
            is_free = 'TRUE'
        elif is_free_raw in ('false', '0'):
            is_free = 'FALSE'
        else:
            is_free = 'TRUE' if price == 0.0 else 'FALSE'

        dubbed     = parse_array_field(row['full_audio_languages'])
        translated = parse_array_field(row['supported_languages'])
        genres     = parse_array_field(row['genres'])
        categories = parse_array_field(row['categories'])

        rows.append({
            'app_id':               int(row['appid']),
            'name':                 str(row['name']).replace('\t', ' ').replace('\n', ' '),
            'required_age':         int(row['required_age']) if str(row['required_age']).isdigit() else 0,
            'is_free':              is_free,
            'dlc_count':            int(row['dlc_count']) if str(row['dlc_count']).isdigit() else 0,
            'game_engine':          str(row['game_engine']).strip(),
            'translated_languages': ', '.join(translated) if translated else '',
            'dubbed_languages':     ', '.join(dubbed)     if dubbed     else '',
            'genres':               ', '.join(genres)     if genres     else '',
            'categories':           ', '.join(categories) if categories else ''
        })
    filepath = f"{TMP_PATH}/dim_game.tsv"
    write_tsv(filepath, rows, cols)
    load_tsv_to_orc("dim_game", filepath, cols)
    print(f"dim_game cargada: {len(rows)} registros")


def cargar_fact_game_release():
    setup()
    import pandas as pd
    from datetime import datetime as dt

    df_games = pd.read_csv(CSV_GAMES, low_memory=False)
    df_gp    = pd.read_csv(CSV_GAME_PLAT)
    df_devs  = pd.read_csv(CSV_DEVELOPERS)
    df_pubs  = pd.read_csv(CSV_PUBLISHERS)

    cols = [
        'app_id', 'date_id', 'developer_id', 'publisher_id', 'platform_id',
        'price', 'price_range', 'positive', 'negative',
        'estimated_owners_min', 'estimated_owners_max',
        'peak_ccu', 'pct_pos_total', 'metacritic_score', 'achievements'
    ]

    dev_name_to_id = dict(zip(df_devs['name'].astype(str), df_devs['developer_id'].astype(int)))
    pub_name_to_id = dict(zip(df_pubs['name'].astype(str), df_pubs['publisher_id'].astype(int)))

    date_id_map = {}
    date_id = 1
    for fecha_str in df_games['release_date'].dropna().unique():
        fecha_str = str(fecha_str).strip()
        try:
            dt.strptime(fecha_str, '%Y-%m-%d')
            if fecha_str not in date_id_map:
                date_id_map[fecha_str] = date_id
                date_id += 1
        except Exception:
            continue

    game_platforms = {}
    for _, row in df_gp.iterrows():
        app_id  = int(row['app_id'])
        plat_id = int(row['platform_id'])
        if app_id not in game_platforms:
            game_platforms[app_id] = []
        game_platforms[app_id].append(plat_id)

    rows = []
    for _, row in df_games.iterrows():
        app_id    = int(row['appid'])
        fecha_str = str(row['release_date']).strip()
        d_id      = date_id_map.get(fecha_str)
        if d_id is None:
            continue

        devs      = parse_array_field(row['developers']) or ['Unknown']
        pubs      = parse_array_field(row['publishers']) or ['Unknown']
        platforms = game_platforms.get(app_id, [1])

        owners_min, owners_max = parse_owners(row['estimated_owners'])

        try:
            pct = float(row['pct_pos_total'])
            pct = '' if pct == -1 else str(round(pct, 4))
        except Exception:
            pct = ''

        try:
            price = float(row['price'])
        except Exception:
            price = 0.0

        try:
            positive = int(row['positive'])
        except Exception:
            positive = 0

        try:
            negative = int(row['negative'])
        except Exception:
            negative = 0

        try:
            peak_ccu = int(row['peak_ccu'])
        except Exception:
            peak_ccu = 0

        try:
            meta = int(row['metacritic_score'])
            meta = '' if meta == 0 else str(meta)
        except Exception:
            meta = ''

        try:
            achievements = int(row['achievements'])
        except Exception:
            achievements = 0

        price_range = get_price_range(price)

        for dev_name in devs:
            dev_id = dev_name_to_id.get(dev_name)
            if dev_id is None:
                continue
            for pub_name in pubs:
                pub_id = pub_name_to_id.get(pub_name)
                if pub_id is None:
                    continue
                for plat_id in platforms:
                    rows.append({
                        'app_id':               app_id,
                        'date_id':              d_id,
                        'developer_id':         dev_id,
                        'publisher_id':         pub_id,
                        'platform_id':          plat_id,
                        'price':                price,
                        'price_range':          price_range,
                        'positive':             positive,
                        'negative':             negative,
                        'estimated_owners_min': owners_min,
                        'estimated_owners_max': owners_max,
                        'peak_ccu':             peak_ccu,
                        'pct_pos_total':        pct,
                        'metacritic_score':     meta,
                        'achievements':         achievements
                    })

    filepath = f"{TMP_PATH}/fact_game_release.tsv"
    write_tsv(filepath, rows, cols)
    load_tsv_to_orc("fact_game_release", filepath, cols)
    print(f"fact_game_release cargada: {len(rows)} registros")


def verificar_datos():
    """Verifica que todas las tablas tienen datos."""
    tablas = [
        'dim_date', 'dim_game', 'dim_developer',
        'dim_publisher', 'dim_platform', 'fact_game_release'
    ]
    for tabla in tablas:
        resultado = run_hive(f"USE {HIVE_DB}; SELECT * FROM {tabla} LIMIT 3;")
        print(f"{tabla}: {resultado.strip()}")


# ============================================================
# DEFINICIÓN DEL DAG
# ============================================================

with DAG(
    dag_id="etl_games_v2",
    description="ETL Games Data Warehouse V2 - ORC con genres y categories en dim_game",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="cargar_dim_platform",      python_callable=cargar_dim_platform)
    t2 = PythonOperator(task_id="cargar_dim_developer",     python_callable=cargar_dim_developer)
    t3 = PythonOperator(task_id="cargar_dim_publisher",     python_callable=cargar_dim_publisher)
    t4 = PythonOperator(task_id="cargar_dim_date",          python_callable=cargar_dim_date)
    t5 = PythonOperator(task_id="cargar_dim_game",          python_callable=cargar_dim_game)
    t6 = PythonOperator(task_id="cargar_fact_game_release", python_callable=cargar_fact_game_release)
    t7 = PythonOperator(task_id="verificar_datos",          python_callable=verificar_datos)

    [t1, t2, t3, t4, t5] >> t6 >> t7 

