import pymysql,csv,json
from datetime import datetime




conn = pymysql.connect(host='', port=, user='', passwd='', db='', autocommit=True) #setup credentials
cur = conn.cursor(pymysql.cursors.DictCursor)

with open('raw_fighter_details.csv') as f:
    fdata = [{k: str(v) for k, v in row.items()}
        for row in csv.DictReader(f, skipinitialspace=True)]
#truncate table and insert into if new data is updated in the files
#cur.execute('TRUNCATE TABLE `ufc_Fighters`')
#cur.execute('TRUNCATE TABLE `ufc_Fight`')

#if we choose to create table right in the program
d = "DROP TABLE ufc_Fighters;"
cur.execute(d)

#create table not necessiary, tables created directly in mysql
d = '''
CREATE TABLE ufc_Fighters  (
`fid` INT(4) NOT NULL AUTO_INCREMENT,
`name` VARCHAR(30) DEFAULT NULL,
`dob` VARCHAR(15) DEFAULT NULL,
`height` VARCHAR(5) DEFAULT NULL,
`weight_lbs` INT(3) DEFAULT NULL,
`stance` VARCHAR(12) DEFAULT NULL,
`reach_in` INT(2) DEFAULT NULL)
ENGINE = INNODB DEFAULT CHARSET = utf8;
'''
cur.execute(d)

for row in fdata:
    name = row['fighter_name']

    dob = row['DOB']
    if dob == "":
        dob_fill = None
    else:
        dob_fill = dob

    height = row['Height'].replace(" ","")
    if height == "":
        height_fill = None
    else:
        height_fill = height

    weight = row['Weight'][0:3]
    if weight == "":
        weight_formatted = None
    else:
        weight_formatted = int(weight)

    stance = row['Stance']
    if stance == "":
        stance_fill = None
    else:
        stance_fill = stance

    reach = row['Reach'][0:2]
    if reach == "":
        reach_formatted = None
    else:
        reach_formatted = int(reach)

    sql = '''INSERT INTO ufc_Fighters (`name`,`dob`,`height`,`weight_lbs`,`stance`,`reach_in`)
    VALUES (%s,%s,%s,%s,%s,%s)'''
    cur.execute(sql,(name,dob_fill,height_fill,weight_formatted,stance_fill,reach_formatted))

with open('raw_total_fight_data.csv') as fa:
    tfdata = [{j: str(c) for j, c in row.items()}
        for row in csv.DictReader(fa, skipinitialspace=True)]

for row in tfdata:
    srow = str(row)
    srow = srow.split(';')

    winner = str(srow[3])
    winner = winner.split(',')
    winner = winner[0][:-3]

    #other info if wanting to user
    info = str(row)
    info = info.split(':')
    info = info[2]
    info = str(info).split(';')

    year = str(srow[0][-4:])

    location = str(srow[1])

    format = str(srow[2])

    rfighter = info[0][2:]
    bfighter = info[1]


    sql = '''INSERT INTO ufc_Fight (`red_fighter`,`blue_fighter`,`format`,`year`,`winner`)
    VALUES (%s,%s,%s,%s,%s)'''
    cur.execute(sql,(rfighter,bfighter,format,year,winner))

#selecting selected data from user unput
sql = '''SELECT * FROM ufc_Fighters WHERE `name` LIKE %s LIMIT 0,1'''
f = raw_input("select fighter 1: ")
cur.execute(sql,(f))

#creating our dictionary to store selected data
d = {}
d['fighter'] = []

#creating fields and entering the data that will end up being appended
for row in cur:
    li = {}
    li['name'] = str(row['name'])
    fighter1 = str(row['name'])
    li['dob'] = str(row['dob'])
    li['height'] = str(row['height'])
    li['weight'] = str(row['weight_lbs'])
    li['stance'] = str(row['stance'])
    li['reach'] = str(row['reach_in'])

#creating win record in format win-loss (#-#)
sql = '''SELECT * FROM ufc_Fight WHERE `red_fighter` LIKE %s OR `blue_fighter` LIKE %s'''
cur.execute(sql,(fighter1,fighter1))
wrecord1 = 0
lrecord1 = 0
for row in cur:
    winner = str(row['winner'])
    if winner == fighter1:
        wrecord1 += 1
    else:
        lrecord1 += 1
record1 = str(wrecord1) + '-' + str(lrecord1)
li['record'] = record1

#win percentage calculation
wp1 = float(wrecord1) / float((wrecord1 + lrecord1))

#selecting and creating records for opponents of the selected fighter
#and the records of the opponent opponents records
sql = '''SELECT * FROM ufc_Fight'''
cur.execute(sql)
for row in cur:
    if row['red_fighter'] == fighter1:
        sql = '''SELECT * FROM ufc_Fight WHERE `red_fighter` LIKE %s OR `blue_fighter` LIKE %s'''
        cur.execute(sql,(row['blue_fighter'],row['blue_fighter']))
        wrecord_o1 = 0
        lrecord_o1 = 0
        for row in cur:
            winnerb = str(row['winner'])
            opponentsb = row['blue_fighter']

            if winnerb == opponentsb:
                wrecord_o1 += 1
            else:
                lrecord_o1 += 1
            record_o1 = str(wrecord_o1) + "-" + str(lrecord_o1)

        sql = '''SELECT * FROM ufc_Fight'''
        cur.execute(sql)
        for row in cur:
            if row['red_fighter'] == opponentsb:
                sql = '''SELECT * FROM ufc_Fight WHERE `red_fighter` LIKE %s OR `blue_fighter` LIKE %s'''
                cur.execute(sql,(row['blue_fighter'],row['blue_fighter']))
                wrecord_o3 = 0
                lrecord_o3 = 0
                for row in cur:
                    winnerbb = str(row['winner'])
                    opponentsbb = row['blue_fighter']

                    if winnerbb == opponentsbb:
                        wrecord_o3 += 1
                    else:
                        lrecord_o3 += 1
                    record_o3 = str(wrecord_o3) + "-" + str(lrecord_o3)
        sql = '''SELECT * FROM ufc_Fight'''
        cur.execute(sql)
        for row in cur:
            if row['blue_fighter'] == opponentsb:
                sql = '''SELECT * FROM ufc_Fight WHERE `red_fighter` LIKE %s OR `blue_fighter` LIKE %s'''
                cur.execute(sql,(row['red_fighter'],row['red_fighter']))
                wrecord_o4 = 0
                lrecord_o4 = 0
                for row in cur:
                    winnerbb = str(row['winner'])
                    opponentsbb = row['red_fighter']

                    if winnerbb == opponentsbb:
                        wrecord_o4 += 1
                    else:
                        lrecord_o4 += 1
                    record_o4 = str(wrecord_o4) + "-" + str(lrecord_o4)





sql = '''SELECT * FROM ufc_Fight'''
cur.execute(sql)
for row in cur:
    if row['blue_fighter'] == fighter1:
        sql = '''SELECT * FROM ufc_Fight WHERE `red_fighter` LIKE %s OR `blue_fighter` LIKE %s'''
        cur.execute(sql,(row['red_fighter'],row['red_fighter']))
        wrecord_o2 = 0
        lrecord_o2 = 0
        for row in cur:
            winnerr = str(row['winner'])
            opponentsr = row['red_fighter']

            if winnerr == opponentsr:
                wrecord_o2 += 1
            else:
                lrecord_o2 += 1
            record_o2 = str(wrecord_o2) + "-" + str(lrecord_o2)
        sql = '''SELECT * FROM ufc_Fight'''
        cur.execute(sql)
        for row in cur:
            if row['red_fighter'] == opponentsr:
                sql = '''SELECT * FROM ufc_Fight WHERE `red_fighter` LIKE %s OR `blue_fighter` LIKE %s'''
                cur.execute(sql,(row['blue_fighter'],row['blue_fighter']))
                wrecord_o5 = 0
                lrecord_o5 = 0
                for row in cur:
                    winnerbr = str(row['winner'])
                    opponentsrr = row['blue_fighter']

                    if winnerbr == opponentsrr:
                        wrecord_o5 += 1
                    else:
                        lrecord_o5 += 1
                    record_o5 = str(wrecord_o5) + "-" + str(lrecord_o5)
        sql = '''SELECT * FROM ufc_Fight'''
        cur.execute(sql)
        for row in cur:
            if row['blue_fighter'] == opponentsr:
                sql = '''SELECT * FROM ufc_Fight WHERE `red_fighter` LIKE %s OR `blue_fighter` LIKE %s'''
                cur.execute(sql,(row['red_fighter'],row['red_fighter']))
                wrecord_o6 = 0
                lrecord_o6 = 0
                for row in cur:
                    winnerbr = str(row['winner'])
                    opponentsrr = row['red_fighter']

                    if winnerbr == opponentsrr:
                        wrecord_o6 += 1
                    else:
                        lrecord_o6 += 1
                    record_o6 = str(wrecord_o6) + "-" + str(lrecord_o6)



#opp win percentage calculation
win_opp1 = wrecord_o1 + wrecord_o2
loss_opp1 = lrecord_o1 + lrecord_o2
owp1 = float(win_opp1)/(float(loss_opp1+win_opp1))

#opp opp win percentage calculation
win_oowp1 = wrecord_o3 + wrecord_o4 + wrecord_o5 + wrecord_o6
loss_oowp1 = lrecord_o3 + lrecord_o4 + lrecord_o5 + lrecord_o6
oowp1 = float(win_oowp1)/(float(loss_oowp1+win_oowp1))

#Ratings Percentage index
rpi1 = (wp1*0.25)+(owp1*0.50)+(oowp1*0.25)
li['rpi'] = str(rpi1)

#appending all data
d['fighter'].append(li)
json.dumps(d,indent=True)

#same process as above except for figher 2
sql = '''SELECT * FROM ufc_Fighters WHERE `name` LIKE %s LIMIT 0,1'''
g = raw_input("select fighter 2: ")
cur.execute(sql,(g))

for row in cur:
    li = {}
    li['name'] = str(row['name'])
    fighter2 = str(row['name'])
    li['dob'] = str(row['dob'])
    li['height'] = str(row['height'])
    li['weight'] = str(row['weight_lbs'])
    li['stance'] = str(row['stance'])
    li['reach'] = str(row['reach_in'])



sql = '''SELECT * FROM ufc_Fight WHERE `red_fighter` LIKE %s OR `blue_fighter` LIKE %s'''
cur.execute(sql,(fighter2,fighter2))
wrecord2 = 0
lrecord2 = 0
for row in cur:
    winner = str(row['winner'])
    if winner == fighter2:
        wrecord2 += 1
    else:
        lrecord2 += 1
record2 = str(wrecord2) + '-' + str(lrecord2)
d['fighter'].append(li)
li['record'] = record2

#win percentage calculation
wp2 = float(wrecord2) / float((wrecord2 + lrecord2))

sql = '''SELECT * FROM ufc_Fight'''
cur.execute(sql)
for row in cur:
    if row['red_fighter'] == fighter1:
        sql = '''SELECT * FROM ufc_Fight WHERE `red_fighter` LIKE %s OR `blue_fighter` LIKE %s'''
        cur.execute(sql,(row['blue_fighter'],row['blue_fighter']))
        wrecord_bo1 = 0
        lrecord_bo1 = 0
        for row in cur:
            winnerb = str(row['winner'])
            opponentsb = row['blue_fighter']

            if winnerb == opponentsb:
                wrecord_bo1 += 1
            else:
                lrecord_bo1 += 1
            record_o1 = str(wrecord_o1) + "-" + str(lrecord_o1)

        sql = '''SELECT * FROM ufc_Fight'''
        cur.execute(sql)
        for row in cur:
            if row['red_fighter'] == opponentsb:
                sql = '''SELECT * FROM ufc_Fight WHERE `red_fighter` LIKE %s OR `blue_fighter` LIKE %s'''
                cur.execute(sql,(row['blue_fighter'],row['blue_fighter']))
                wrecord_bo3 = 0
                lrecord_bo3 = 0
                for row in cur:
                    winnerbb = str(row['winner'])
                    opponentsbb = row['blue_fighter']

                    if winnerbb == opponentsbb:
                        wrecord_bo3 += 1
                    else:
                        lrecord_bo3 += 1
                    record_o3 = str(wrecord_o3) + "-" + str(lrecord_o3)
        sql = '''SELECT * FROM ufc_Fight'''
        cur.execute(sql)
        for row in cur:
            if row['blue_fighter'] == opponentsb:
                sql = '''SELECT * FROM ufc_Fight WHERE `red_fighter` LIKE %s OR `blue_fighter` LIKE %s'''
                cur.execute(sql,(row['red_fighter'],row['red_fighter']))
                wrecord_bo4 = 0
                lrecord_bo4 = 0
                for row in cur:
                    winnerbb = str(row['winner'])
                    opponentsbb = row['red_fighter']

                    if winnerbb == opponentsbb:
                        wrecord_bo4 += 1
                    else:
                        lrecord_bo4 += 1
                    record_bo4 = str(wrecord_bo4) + "-" + str(lrecord_bo4)





sql = '''SELECT * FROM ufc_Fight'''
cur.execute(sql)
for row in cur:
    if row['blue_fighter'] == fighter1:
        sql = '''SELECT * FROM ufc_Fight WHERE `red_fighter` LIKE %s OR `blue_fighter` LIKE %s'''
        cur.execute(sql,(row['red_fighter'],row['red_fighter']))
        wrecord_bo2 = 0
        lrecord_bo2 = 0
        for row in cur:
            winnerr = str(row['winner'])
            opponentsr = row['red_fighter']

            if winnerr == opponentsr:
                wrecord_bo2 += 1
            else:
                lrecord_bo2 += 1
            record_bo2 = str(wrecord_bo2) + "-" + str(lrecord_bo2)
        sql = '''SELECT * FROM ufc_Fight'''
        cur.execute(sql)
        for row in cur:
            if row['red_fighter'] == opponentsr:
                sql = '''SELECT * FROM ufc_Fight WHERE `red_fighter` LIKE %s OR `blue_fighter` LIKE %s'''
                cur.execute(sql,(row['blue_fighter'],row['blue_fighter']))
                wrecord_bo5 = 0
                lrecord_bo5 = 0
                for row in cur:
                    winnerbr = str(row['winner'])
                    opponentsrr = row['blue_fighter']

                    if winnerbr == opponentsrr:
                        wrecord_bo5 += 1
                    else:
                        lrecord_bo5 += 1
                    record_bo5 = str(wrecord_bo5) + "-" + str(lrecord_bo5)
        sql = '''SELECT * FROM ufc_Fight'''
        cur.execute(sql)
        for row in cur:
            if row['blue_fighter'] == opponentsr:
                sql = '''SELECT * FROM ufc_Fight WHERE `red_fighter` LIKE %s OR `blue_fighter` LIKE %s'''
                cur.execute(sql,(row['red_fighter'],row['red_fighter']))
                wrecord_bo6 = 0
                lrecord_bo6 = 0
                for row in cur:
                    winnerbr = str(row['winner'])
                    opponentsrr = row['red_fighter']

                    if winnerbr == opponentsrr:
                        wrecord_bo6 += 1
                    else:
                        lrecord_bo6 += 1
                    record_bo6 = str(wrecord_bo6) + "-" + str(lrecord_bo6)



#opp win percentage calculation
win_opp2 = wrecord_bo1 + wrecord_bo2
loss_opp2 = lrecord_bo1 + lrecord_bo2
owp2 = float(win_opp2)/(float(loss_opp2+win_opp2))

#opp opp win percentage calculation
win_oowp2 = wrecord_bo3 + wrecord_bo4 + wrecord_bo5 + wrecord_bo6
loss_oowp2 = lrecord_bo3 + lrecord_bo4 + lrecord_bo5 + lrecord_bo6
oowp2 = float(win_oowp2)/(float(loss_oowp2+win_oowp2))

#Ratings Percentage index
rpi2 = (wp2*0.25)+(owp2*0.50)+(oowp2*0.25)
li['rpi'] = str(rpi2)
print json.dumps(d,indent=True)


cur.close()
conn.close()
