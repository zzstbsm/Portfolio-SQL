def next_day(current_date):
    days_in_month = {
                    "01" : 31,
                    "02" : 28,
                    "03" : 31,
                    "04" : 30,
                    "05" : 31,
                    "06" : 30,
                    "07" : 31,
                    "08" : 31,
                    "09" : 30,
                    "10" : 31,
                    "11" : 30,
                    "12" : 31,
                    }
                    
    year = int(current_date[0])
    month = int(current_date[1])
    day = int(current_date[2])
    
    leap_condition = year%4
    leap_year = False
    if leap_condition==0:
        leap_year = True
    
    # Flag if false if it is February 29th and it is a leap year
    modify = True
    
    day += 1
    if day > days_in_month[current_date[1]]:
        if month == 2:
            if leap_year and day==29:
                modify = False
        if modify:
            day = 1
            month +=1
    
    if month > 12:
        month = 1
        year+=1
    
    year = "{0:0>4}".format(year)
    month = "{0:0>2}".format(month)
    day = "{0:0>2}".format(day)
    
    return [year,month,day]

def format_date(current_date):
    return "{0:0>4}{1:0>2}{2:0>2}".format(current_date[0],current_date[1],current_date[2])

def update_table_infezioni(db):
    
    # Resume from the day after the last registered day
    db.execute("SELECT MAX(data) FROM infezioni")
    start_date = db.cursor.fetchall()[0][0]
    if start_date == None:
        start_date = "2020-02-23"
    else:
        start_date = str(start_date)
    end_date = "2022-11-11"
    start_date = start_date.split("-")
    end_date = end_date.split("-")
    
    current_date = next_day(start_date)
    print("Carico dal giorno %s" %("-".join(current_date)))
    
    # Cycle while the current date is different from the end date
    while (not (
            current_date[0] == end_date[0] and
            current_date[1] == end_date[1] and
            current_date[2] == end_date[2]
            ) ):
        
        strdate = format_date(current_date)
        db.update_infezioni("https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-regioni/dpc-covid19-ita-regioni-%s.csv" %strdate)
        print("Aggiornato alla data %s" %strdate)
        current_date = next_day(current_date)