from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import col, when
import unicodedata
import pandas as pd

hechos_files = ["Hechos_2013.csv", 
                "Hechos_2014.csv", 
                "Hechos_2015.csv", 
                "Hechos_2016.csv",
                "Hechos_2017.csv",
                "Hechos_2018.csv",
                "Hechos_2019.csv",
                "Hechos_2020.csv",
                "Hechos_2021.csv",
                "Hechos_2022.csv",
                "Hechos_2023.csv"]
lesionados_files = ["Lesionados_2013.csv",
                    "Lesionados_2014.csv",
                    "Lesionados_2015.csv",
                    "Lesionados_2016.csv",
                    "Lesionados_2017.csv",
                    "Lesionados_2018.csv",
                    "Lesionados_2019.csv",
                    "Lesionados_2020.csv",
                    "Lesionados_2021.csv",
                    "Lesionados_2022.csv",
                    "Lesionados_2023.csv"]

vehiculos_files = [
                    "Vehículos_2013.csv",
                    "Vehículos_2014.csv",
                    "Vehículos_2015.csv",
                    "Vehículos_2016.csv",
                    "Vehículos_2017.csv",
                    "Vehículos_2018.csv",
                    "Vehículos_2019.csv",
                    "Vehículos_2020.csv",
                    "Vehículos_2021.csv",
                    "Vehículos_2022.csv",
                    "Vehículos_2023.csv"
                    ]



def normalize_col(colname: str) -> str:
    colname = colname.strip().lower()
    colname = unicodedata.normalize('NFKD', colname).encode('ascii', 'ignore').decode('utf-8')
    return colname

hechos_renames = {
    "num_hecho": "num_corre", "num_correlativo": "num_corre", "num_corre": "num_corre", "num": "num_corre", "nro_hecho": "num_corre",
    "dia_ocu": "dia_ocu", "dia": "dia_ocu", "dia_ocurrencia": "dia_ocu",
    "mes_ocu": "mes_ocu", "mes": "mes_ocu",
    "anio_ocu": "anio_ocu", "ano_ocu": "anio_ocu", "año_ocu": "anio_ocu",
    "hora_ocu": "hora_ocu", "hora": "hora_ocu",
    "g_hora": "g_hora", "g_hora_5": "g_hora_5",
    "areag_ocu": "area_geo_ocu", "area_geo_ocu": "area_geo_ocu",
    "depto_ocu": "depto_ocu", "mupio_ocu": "mupio_ocu", "zona_ocu": "zona_ocu",
    "sexo_pil": "sexo", "sexo_con": "sexo", "sexo_per": "sexo",
    "edad_pil": "edad", "edad_con": "edad", "edad_per": "edad",
    "estado_pil": "estado_con", "estado_con": "estado_con",
    "marca_veh": "marca_veh", "tipo_veh": "tipo_veh", "color_veh": "color_veh", "modelo_veh": "modelo_veh",
    "g_modelo_veh": "g_modelo_veh", "tipo_eve": "tipo_eve"
}

vehiculos_renames = hechos_renames 

hechos_final_cols = [
    "num_corre", "anio_ocu", "mes_ocu", "dia_ocu", "dia_sem_ocu", "hora_ocu", "g_hora", "g_hora_5",
    "depto_ocu", "mupio_ocu", "area_geo_ocu", "zona_ocu", "sexo", "edad", "mayor_menor",
    "estado_con", "tipo_veh", "marca_veh", "color_veh", "modelo_veh", "g_modelo_veh", "tipo_eve", "causa_acc"
]

vehiculos_final_cols = hechos_final_cols[:-1]  

def normalizar_df(path, year, renames, final_cols):
    df = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/data" + path))

    df = df.toDF(*[normalize_col(c) for c in df.columns])

    for old, new in renames.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)


    if "anio_ocu" not in df.columns:
        df = df.withColumn("anio_ocu", lit(year))

    for c in final_cols:
        if c not in df.columns:
            df = df.withColumn(c, lit(None))
    df = df.select(final_cols)
    return df

hechos_dfs = [normalizar_df(f, year, hechos_renames, hechos_final_cols) for f, year in zip(hechos_files, range(2013, 2024))]
hechos_general = hechos_dfs[0]
for df in hechos_dfs[1:]:
    hechos_general = hechos_general.unionByName(df, allowMissingColumns=True)

vehiculos_dfs = [normalizar_df(f, year, vehiculos_renames, vehiculos_final_cols) for f, year in zip(vehiculos_files, range(2013, 2024))]
vehiculos_general = vehiculos_dfs[0]
for df in vehiculos_dfs[1:]:
    vehiculos_general = vehiculos_general.unionByName(df, allowMissingColumns=True)



vehiculos_clean = vehiculos_general.filter(
    col("anio_ocu").isNotNull() &
    col("mes_ocu").isNotNull() &
    col("dia_sem_ocu").isNotNull() &
    col("dia_ocu").isNotNull() &
    col("hora_ocu").isNotNull() &
    col("depto_ocu").isNotNull() &
    col("mupio_ocu").isNotNull() &
    col("area_geo_ocu").isNotNull() &
    col("zona_ocu").isNotNull() &
    col("sexo").isNotNull() &
    col("edad").isNotNull() &
    col("estado_con").isNotNull() &
    col("tipo_veh").isNotNull() &
    col("marca_veh").isNotNull() &
    col("color_veh").isNotNull() &
    col("modelo_veh").isNotNull() &
    col("g_modelo_veh").isNotNull() &
    col("tipo_eve").isNotNull() &
    col("g_hora").isNotNull() &
    col("g_hora_5").isNotNull()
)

hechos_clean = hechos_general.filter(
    col("anio_ocu").isNotNull() &
    col("mes_ocu").isNotNull() &
    col("dia_sem_ocu").isNotNull() &
    col("dia_ocu").isNotNull() &
    col("hora_ocu").isNotNull() &
    col("depto_ocu").isNotNull() &
    col("mupio_ocu").isNotNull() &
    col("area_geo_ocu").isNotNull() &
    col("zona_ocu").isNotNull() &
    col("sexo").isNotNull() &
    col("edad").isNotNull() &
    col("estado_con").isNotNull() &
    col("tipo_veh").isNotNull() &
    col("marca_veh").isNotNull() &
    col("color_veh").isNotNull() &
    col("modelo_veh").isNotNull() &
    col("g_modelo_veh").isNotNull() &
    col("tipo_eve").isNotNull() &    
    col("g_hora").isNotNull() &
    col("g_hora_5").isNotNull()
)


vehiculos_clean.createOrReplaceTempView("vehiculos")
hechos_clean.createOrReplaceTempView("hecho")

joined = spark.sql("""SELECT h.num_corre as id_hecho, v.*,  h.causa_acc, h.mayor_menor, h.estado_con  FROM  vehiculos v join hecho h ON v.anio_ocu = h.anio_ocu AND v.mes_ocu = h.mes_ocu 
                   AND v.dia_sem_ocu = h.dia_sem_ocu AND v.dia_ocu = h.dia_ocu AND v.hora_ocu = h.hora_ocu 
                   AND v.depto_ocu = h.depto_ocu AND v.mupio_ocu = h.mupio_ocu AND v.area_geo_ocu = h.area_geo_ocu 
                   AND v.zona_ocu = h.zona_ocu AND v.sexo = h.sexo AND v.edad = h.edad AND v.estado_con = h.estado_con 
                   AND v.tipo_veh = h.tipo_veh AND v.marca_veh = h.marca_veh AND v.color_veh = h.color_veh 
                   AND v.modelo_veh = h.modelo_veh AND v.g_modelo_veh = h.g_modelo_veh AND v.tipo_eve = h.tipo_eve
                   AND v.g_hora = h.g_hora AND v.g_hora_5 = h.g_hora_5""")

joined.write.mode("overwrite").option("header", "true").csv("data/vehiculos_hechos")



lesionados_renames = {
    "num_hecho": "num_corre",
    "num_correlativo": "num_corre",
    "num_corre": "num_corre",
    "Num_corre": "num_corre",
    "año_ocu": "anio_ocu",
    "ano_ocu": "anio_ocu",
    "anio_ocu": "anio_ocu",
    "dia_ocu": "dia_ocu",
    "dia": "dia_ocu",
    "dia_sem_ocu": "dia_sem_ocu",
    "mes_ocu": "mes_ocu",
    "mes": "mes_ocu",
    "hora_ocu": "hora_ocu",
    "hora": "hora_ocu",
    "g_hora": "g_hora",
    "g_hora_5": "g_hora_5",
    "depto_ocu": "depto_ocu",
    "mupio_ocu": "mupio_ocu",
    "zona_ocu": "zona_ocu",
    "zona_ciudad": "zona_ciudad",
    "areag_ocu": "area_geo_ocu",
    "area_geo_ocu": "area_geo_ocu",
    "sexo_pil": "sexo",
    "sexo_per": "sexo",
    "sexo_vic": "sexo",
    "edad_pil": "edad",
    "edad_per": "edad",
    "edad_vic": "edad",
    "g_edad": "g_edad",
    "g_edad_2": "g_edad_2",
    "edad_quinquenales": "edad_quinquenales",
    "g_edad_60ymas": "g_edad_60ymas",
    "g_edad_80ymas": "g_edad_80ymas",
    "mayor_menor": "mayor_menor",
    "Otro_g_edad_fall_les": "otro_g_edad_fall_les",
    "tipo_veh": "tipo_veh",
    "marca_veh": "marca_veh",
    "color_veh": "color_veh",
    "modelo_veh": "modelo_veh",
    "g_modelo_veh": "g_modelo_veh",
    "tipo_eve": "tipo_eve",
    "fall_les": "fall_les",
    "Fallecidos_Lesionados": "fall_les",
    "int_o_noint": "int_o_noint",
    "g_edad_80ymas": "g_edad_80ymas",
    "g_edad_60ymas": "g_edad_60ymas"
}

lesionados_final_cols = [
    "num_corre",          
    "anio_ocu",          
    "mes_ocu",            
    "dia_ocu",           
    "dia_sem_ocu",        
    "hora_ocu",           
    "g_hora",            
    "g_hora_5",           
    "depto_ocu",          
    "mupio_ocu",         
    "area_geo_ocu",       
    "zona_ocu",           
    "zona_ciudad",        
    "sexo",               
    "edad",               
    "g_edad",   
    "mayor_menor",        
    "tipo_veh",           
    "marca_veh",        
    "color_veh",          
    "modelo_veh",         
    "g_modelo_veh",       
    "tipo_eve",           
    "fall_les",           
    "int_o_noint",        
    "otro_g_edad_fall_les",
    "tipo_eve_cod"
]


lesionados_dfs = []
for f, year in zip(lesionados_files, range(2013, 2024)):
    df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/workspace/default/lab8_new/" + f)
    )
    if year == 2014:
        for c in ["num_corre", "corre_base"]:
            if c in df.columns:
                df = df.drop(c)
    df = normalizar_df(f, year, lesionados_renames, lesionados_final_cols)
    lesionados_dfs.append(df)



lesionados_general = lesionados_dfs[0]
for df in lesionados_dfs[1:]:
    lesionados_general = lesionados_general.unionByName(df, allowMissingColumns=True)

lesionados_general.createOrReplaceTempView("lesionados")

joined2 = spark.sql("""SELECT h.num_corre as id_hecho, v.*,  h.causa_acc, h.mayor_menor, h.estado_con  FROM  lesionados v join hecho h ON v.anio_ocu = h.anio_ocu AND v.mes_ocu = h.mes_ocu 
                   AND v.dia_sem_ocu = h.dia_sem_ocu AND v.dia_ocu = h.dia_ocu AND v.hora_ocu = h.hora_ocu 
                   AND v.depto_ocu = h.depto_ocu AND v.mupio_ocu = h.mupio_ocu AND v.area_geo_ocu = h.area_geo_ocu 
                   AND v.zona_ocu = h.zona_ocu AND v.sexo = h.sexo AND v.edad = h.edad AND  v.tipo_veh = h.tipo_veh AND v.marca_veh = h.marca_veh AND v.color_veh = h.color_veh 
                   AND v.modelo_veh = h.modelo_veh AND v.g_modelo_veh = h.g_modelo_veh AND v.tipo_eve = h.tipo_eve
                   AND v.g_hora = h.g_hora AND v.g_hora_5 = h.g_hora_5""")


joined2.write.mode("overwrite").option("header", "true").csv("data/lesionados_hechos")