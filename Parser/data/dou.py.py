import pandas as pd

# --- Archivos a leer ---
files = ["Vehículos_2022.csv", "Vehículos_2023.csv"]

# --- Diccionario de mapeo (texto → número) ---
mapa_g_hora = {
    "00:00 a 05:59": 1,
    "06:00 a 11:59": 2,
    "12:00 a 17:59": 3,
    "18:00 a 23:59": 4,
    "ignorada": 5,
    "Ignorada": 5
}

# --- Procesamiento ---
for file in files:
    print(f"Procesando {file}...")

    # Leer CSV
    df = pd.read_csv(file, encoding="utf-8", sep=None, engine="python")

    # Asegurar que existe g_hora
    if "g_hora" not in df.columns:
        print(f"⚠️ {file} no contiene la columna 'g_hora'. Saltando...\n")
        continue

    # Normalizar texto (quita espacios, tildes, mayúsculas, etc.)
    df["g_hora"] = (
        df["g_hora"]
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace("á", "a")
        .str.replace("í", "i")
    )

    # Intentar convertir a número
    df["g_hora_num"] = pd.to_numeric(df["g_hora"], errors="coerce")

    # Mapear texto a número
    df["g_hora_mapped"] = df["g_hora"].map(mapa_g_hora)

    # Combinar: si ya era número, conservar; si no, usar el mapeo
    df["g_hora_final"] = df["g_hora_num"].combine_first(df["g_hora_mapped"])

    # Reemplazar nulos o valores no mapeados por 5 (Ignorada)
    df["g_hora_final"] = df["g_hora_final"].fillna(5).astype(int)

    # Reemplazar la columna original
    df.drop(columns=["g_hora", "g_hora_num", "g_hora_mapped"], inplace=True)
    df.rename(columns={"g_hora_final": "g_hora"}, inplace=True)

    # Mostrar resumen
    print(df["g_hora"].value_counts().sort_index())

    # Guardar CSV limpio
    output = file.replace(".csv", "_mapped.csv")
    df.to_csv(output, index=False, encoding="utf-8-sig")
    print(f"✅ Archivo guardado sin texto: {output}\n")
