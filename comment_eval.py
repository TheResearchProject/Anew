# -*- coding: utf-8 -*-
import csv
import re
import pdb
import pymysql
import progressbar

anew_csv = csv.DictReader(open('ANEW.csv'))

anew_hash = {}
for row in anew_csv:
    anew_hash[row['Description'].lower()] = {
        'Valence Mean': float(row['Valence Mean']),
        'Valence SD': float(row['Valence SD']),
        'Arousal Mean': float(row['Arousal Mean']),
        'Arousal SD': float(row['Arousal SD'])
    }
anew_hash_keys = anew_hash.keys()    

# Connect to the database
print "Connecting to the database..."
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='Aloimonos',
                             database='news',
                             cursorclass=pymysql.cursors.DictCursor)

pbar_cursor = connection.cursor()
pbar_cursor.execute("SELECT count(*) as countdown from `Comment`")
pbar_result = pbar_cursor.fetchone()
pbar_total_rows = pbar_result['countdown']
pbar_widgets = ['Calculating cat2... ', 
                progressbar.Counter(), 
                '/{0} '.format(pbar_total_rows), 
                progressbar.Percentage(), 
                ' ', 
                progressbar.Bar(marker=progressbar.RotatingMarker()),
                ' ', 
                progressbar.ETA()]

cursor = connection.cursor()

print "Sending query..."
select_sql = "SELECT `id`, `Content` FROM `Comment`"
cursor.execute(select_sql)
update_sql = """
UPDATE `Comment` 
   SET `meanArousal` = %s
      ,`stdvArousal` = %s
      ,`meanValence` = %s
      ,`stdvValence` = %s
   WHERE `id` = %s
"""

print "Starting Analysis...\n"
pbar = progressbar.ProgressBar(widgets=pbar_widgets, maxval=pbar_total_rows).start()
pbar_updated_rows = 0

while True:
    result = cursor.fetchone()
    if result == None:
        #Finish when there are no more rows to fetch
        break
        
    meanArousalSum = 0.0
    stdvArousalSum = 0.0
    meanValenceSum = 0.0
    stdvValenceSum = 0.0
    
    wordsFound = 0
    
    words = re.split("[^\wäöüÄÖÜß]*",result['Content'].lower())
    for word in words:
        if word in anew_hash_keys:
            meanArousalSum += anew_hash[word]['Arousal Mean']
            stdvArousalSum += anew_hash[word]['Arousal SD']
            meanValenceSum += anew_hash[word]['Valence Mean']
            stdvValenceSum += anew_hash[word]['Valence SD']
            wordsFound += 1
            
    if wordsFound >= 3:
        update_item_tuple = (
            meanArousalSum/wordsFound,
            stdvArousalSum/wordsFound,
            meanValenceSum/wordsFound,
            stdvValenceSum/wordsFound,
            result['id'])
        #Update the database
        update_cursor = connection.cursor()
        update_cursor.execute(update_sql, update_item_tuple)       
    
    pbar_updated_rows += 1
    pbar.update(pbar_updated_rows)
pbar.finish()
    
print("Update finished, commiting changes to database.")
#Send commit after all the updates were done correctly
connection.commit()
print "Done!"