import keyring
import mysql.connector
import pandas as pd

host = "185.101.156.105"
usr = "auto1"
pw = keyring.get_password("Database-Values", "db_auto1")
archive_db = mysql.connector.connect(
        host=host,
        user=usr,
        password=pw,
        database="archive"
    )
archive_cursor = archive_db.cursor()
# archive_cursor.execute("SET SESSION MAX_EXECUTION_TIME=180000")
# archive_cursor.execute("SELECT count(*) FROM archive.archive_llm;")
sql_string = f'SELECT count(*) as "Anzahl Artikel", round(avg(word_count), 0) ' \
             f'as "Durchschnittliche Anzahl Wörter pro Text", sum(token_count_openai) ' \
             f'as "Anzahl Tokens gesamt" FROM archive.archive_llm ' \
             f'WHERE copyright_awp = 1;'
archive_cursor.execute(sql_string)
res = archive_cursor.fetchall()
print(pd.DataFrame(res))

# Close DB connection
archive_cursor.close()
archive_db.close()
