import sys

from lib.load_data import update_db
from lib.miscellaneous import update_table_infezioni

db = update_db()
query = "USE covid"
db.execute(query)

# Parte di codice per la tabella delle infezioni
command = input("Devi creare la tabella per le infezioni? (s/[n]) ")
if command == "s":
    db.create_table_infezioni()
    print("Tabella creata")

command = input("Devi aggiornare la tabella delle infezioni? (s/[n]) ")
if command == "s":
    update_table_infezioni(db)

# Parte di codice per la tabella dei vaccini
command = input("Devi creare la tabella per i vaccini? (s/[n]) ")
if command == "s":
    db.create_table_vaccini()
    print("Tabella creata")

command = input("Devi aggiornare la tabella delle  somministrazioni del vaccino? (s/[n]) ")
if command == "s":
    print("Inizio caricamento")
    db.update_vaccini()
    print("Fine caricamento")
db.close()