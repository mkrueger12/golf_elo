import time
import pandas as pd
import numpy as np
from src.data.data_collection import s3readcsv, get_field, sg_data, writeToS3
from src.features.feature_creation import Elo, addPlayerToLeague, trn_sim
from itertools import combinations

# tournament info
sims = 40000
print(sims)
cut_line = 65
tourn_name = 'FarmInsur'

# import data
sg = sg_data(date='2017-09-01')

elo_initial = s3readcsv(bucket_name='golfdfs', bucket_folder='raw-data/elo',
                        filename='data_initial.csv')

# download tournament field
try:
    field = get_field(league='PGA', n=2)
    field = list(field['name'].unique())
    print('FIELD FOUND')
except:
    print('FIELD NOT AVAILABLE')
    field = elo_initial.sample(n=20)
    field = list(field['player'])

# filter on tournament field
elo_initial = elo_initial[elo_initial['player'].isin(field)]
diff = set(field) ^ set(list(elo_initial['player']))
sg = sg[sg['full'].isin(field)]

# add missing players to sg dataset
missing = pd.DataFrame(columns=sg.columns)
missing['full'] = list(diff)
missing.fillna(0, inplace=True)
sg = pd.concat([sg, missing])

# create league
elo_players = list(elo_initial['player'].unique())

eloLeague = Elo(k=.5, g=1)  #.2215

# add players in trn to eloLeague
addPlayerToLeague(field=field, elo_initial=elo_initial, eloLeague=eloLeague, plist=elo_players)

# run simulation
t1 = time.time()

combos = [c for c in combinations(field, 2)]

ran = range(0, len(combos))

for iteration in range(1, sims+1):
    print(iteration)
    results = []
    for player in field:
        x = trn_sim(sg, player, iteration=iteration)
        results.append(x)

    # create df out of np list
    names = ['sg', 'tournament', 'name', 'name1']
    df = pd.DataFrame(data=results, columns=names)
    df = df[['sg', 'name']]
    array = np.array(df)

    for i in ran:
        p1 = combos[i][0]
        p2 = combos[i][1]
        p1_score = array[array[:, 1] == p1][0][0]
        p2_score = array[array[:, 1] == p2][0][0]

        if p1_score > p2_score:
            eloLeague.gameOver(winner=p1, loser=p2)
        else:
            eloLeague.gameOver(winner=p2, loser=p1)


t2 = time.time()

updated = []
for player in field:
    dict = {'player': player,
            'elo': eloLeague.ratingDict[player]}
    updated.append(dict)

df = pd.DataFrame.from_dict(updated)

print((t2-t1))

df.sort_values('elo', inplace=True, ascending=False)
df['rank'] = df['elo'].rank(ascending=False).astype(int)
df['tournament'] = tourn_name

# write to s3
file = 'elo-sim'
BUCKET_FOLDER = f'raw-data/{file}'
writeToS3(data=df, bucket_name='golfdfs', filename='data.csv',
          bucket_folder=BUCKET_FOLDER)
print(file, 'data upload complete')

