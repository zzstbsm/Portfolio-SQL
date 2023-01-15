import sys
import logging

import numpy as np
import pandas as pd

from lib.db_lib import db_lib as db_lib

logger = logging.getLogger(__name__)

class update_db(db_lib):
    
    def __init__(self):
        
        # new = False
        # dbname = "covid"
        super().__init__(new=False,dbname="covid")
        
        self.header_infezioni = [
                    "data",
                    "denominazione_regione",
                    "codice_regione",
                    "ricoverati_con_sintomi",
                    "terapia_intensiva",
                    "totale_ospedalizzati",
                    "isolamento_domiciliare",
                    "totale_positivi",
                    "variazione_totale_positivi",
                    "nuovi_positivi",
                    "dimessi_guariti",
                    "deceduti",
                    "casi_da_sospetto_diagnostico",
                    "casi_da_screening",
                    "totale_casi",
                    "tamponi",
                    "casi_testati",
                    "ingressi_terapia_intensiva"
                    ]
                    
        self.header_vaccinazioni = [
                                    "data",
                                    "denominazione_regione",
                                    "codice_regione",
                                    "area",
                                    "maschi",
                                    "femmine",
                                    "dose_1",
                                    "dose_2",
                                    "infezione_dopo_dose_1",
                                    "booster_1",
                                    "booster_2"
                                    ]
        
        self.regioni = {
                        "Abruzzo" : "ABR",
                        "Basilicata" : "BAS",
                        "Calabria" : "CAL",
                        "Campania" : "CAM",
                        "Emilia-Romagna" : "EMR",
                        "Friuli Venezia Giulia" : "FVG",
                        "Lazio" : "LAZ",
                        "Liguria" : "LIG",
                        "Lombardia" : "LOM",
                        "Marche" : "MAR",
                        "Molise" : "MOL",
                        "P.A. Bolzano" : "PAB",
                        "P.A. Trento" : "PAT",
                        "Piemonte" : "PIE",
                        "Puglia" : "PUG",
                        "Sardegna" : "SAR",
                        "Sicilia" : "SIC",
                        "Toscana" : "TOS",
                        "Umbria" : "UMB",
                        "Valle d'Aosta" : "VDA",
                        "Veneto" : "VEN"
                        }
    
    def create_table_infezioni(self):
        """
        Create tables of the database
        """
                
        query = """
                CREATE TABLE infezioni (
                    data DATE NOT NULL,
                    denominazione_regione VARCHAR(50) NOT NULL,
                    codice_regione INT NOT NULL,
                    area CHAR(3) NOT NULL,
                    ricoverati_con_sintomi INT,
                    terapia_intensiva INT,
                    totale_ospedalizzati INT,
                    isolamento_domiciliare INT,
                    totale_positivi INT,
                    variazione_totale_positivi INT,
                    nuovi_positivi INT,
                    dimessi_guariti INT,
                    deceduti INT,
                    casi_da_sospetto_diagnostico INT,
                    casi_da_screening INT,
                    totale_casi INT,
                    tamponi INT,
                    casi_testati INT,
                    ingressi_terapia_intensiva INT
                )
                """
        self.execute(query)
    
    def update_infezioni(self,url):
        
        df = pd.read_csv(url)
        
        length = len(df)
        
        for i in range(length):
            row = []
            header = []
            for col in self.header_infezioni:
                
                cell = df[col][i]
                
                # If cell is a string
                if isinstance(cell,str):
                    if col == "data":
                        data = cell.split("T")[0]
                        cell = data
                    if col == "denominazione_regione":
                        regione = cell
                        area = self.regioni[regione]
                        header.append("area")
                        row.append("".join(['"',area,'"']))
                    cell = "".join(['"',cell,'"'])
                # If cell is a number
                else:
                    if np.isnan(cell):
                        continue
                    cell = str(cell)
                header.append(col)
                row.append(cell)
        
            header = ["(",", ".join(header),")"]
            header = "".join(header)
            row = ["(",", ".join(row),")"]
            row = "".join(row)
            
            query = "INSERT INTO infezioni %s VALUES %s" %(header,row)
            self.execute(query)
        
        self.commit()
        
    def create_table_vaccini(self):
        """
        Create tables of the database
        """
                
        query = """
                CREATE TABLE vaccinazioni (
                    data DATE NOT NULL,
                    denominazione_regione VARCHAR(50) NOT NULL,
                    codice_regione INT NOT NULL,
                    area CHAR(3) NOT NULL,
                    maschi INT,
                    femmine INT,
                    dose_1 INT,
                    dose_2 INT,
                    infezione_dopo_dose_1 INT,
                    booster_1 INT,
                    booster_2 INT
                )
                """
        self.execute(query)
    
    def update_vaccini(self):
        
        to_df_col = {
                    "data" : "data",
                    "denominazione_regione" : "reg",
                    "codice_regione" : "ISTAT",
                    "area" : "area",
                    "maschi" : "m",
                    "femmine" : "f",
                    "dose_1" : "d1",
                    "dose_2" : "d2",
                    "infezione_dopo_dose_1" : "dpi",
                    "booster_1" : "db1",
                    "booster_2" : "db2"
                    }
        
        self.execute("DELETE from vaccinazioni")
        df = pd.read_csv("https://raw.githubusercontent.com/italia/covid19-opendata-vaccini/master/dati/somministrazioni-vaccini-summary-latest.csv")
        
        length = len(df)
        
        for i in range(length):
            row = []
            header = []
            for col in self.header_vaccinazioni:
                
                df_col = to_df_col[col]
                
                cell = df[df_col][i]
                
                # If cell is a string
                if isinstance(cell,str):
                    cell = "".join(['"',cell,'"'])
                # If cell is a number
                else:
                    if np.isnan(cell):
                        continue
                    cell = str(cell)
                header.append(col)
                row.append(cell)
        
            header = ["(",", ".join(header),")"]
            header = "".join(header)
            row = ["(",", ".join(row),")"]
            row = "".join(row)
            
            query = "INSERT INTO vaccinazioni %s VALUES %s" %(header,row)
            self.execute(query)
        
        self.commit()