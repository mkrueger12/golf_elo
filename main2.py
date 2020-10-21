import time
import pandas as pd
from src.data.data_collection import s3readcsv, get_field, sg_data, writeToS3
from src.features.feature_creation import Elo, addPlayerToLeague, createCombos, trn_sim
from itertools import combinations

# tournament info
sims = 10
cut_line = 70
tourn_name = 'ZOZO'

# import data
sg = sg_data(date='2017-09-01')

elo_initial = s3readcsv(bucket_name='golfdfs', bucket_folder='raw-data/elo',
                 filename='data_initial.csv')

# download tournament field
try:
    field = get_field(league='PGA', n=3)
    field = list(field['name'].unique())
    print('FIELD FOUND')
except:
    print('FIELD NOT AVAILABLE')
    field = elo_initial.sample(n=20)
    field = list(field['player'])

# filter on tournament field
elo_initial = elo_initial[elo_initial['player'].isin(field)]
sg = sg[sg['full'].isin(field)]

# create league
elo_players = list(elo_initial['player'].unique())

eloLeague = Elo(k=.2215, g=1)  #.2215

# add players in trn to eloLeague
addPlayerToLeague(field=field, elo_initial=elo_initial, eloLeague=eloLeague, plist=elo_players)

# run simulation
elo_collect = []
all = []
results = []

t1 = time.time()

for iteration in range(1, 10):
    print(iteration)
    plist = []
    for player in field:
        x = trn_sim(sg, player, iteration=iteration)
        results.append(x)
        plist.append(player)

    df = pd.concat(results)

    combos = [c for c in combinations(plist, 2)]

    for c in combos:
        p1 = c[0]
        p2 = c[1]
        p1_score = list(df[df['name'] == p1]['sg'])
        p2_score = list(df[df['name'] == p2]['sg'])

        if p1_score > p2_score:
            eloLeague.gameOver(winner=p1, loser=p2)
        else:
            eloLeague.gameOver(winner=p2, loser=p1)

t2 = time.time()
print((t2-t1)/60)


updated = []
for player in field:
    dict = {'player': player,
            'elo': eloLeague.ratingDict[player]}
    updated.append(dict)

df = pd.DataFrame.from_dict(updated)

df.sort_values('elo', inplace=True, ascending=False)
df['rank'] = df['elo'].rank(ascending=False).astype(int)
df['tournament'] = tourn_name

# write to s3
file = 'elo-sim'
BUCKET_FOLDER = f'raw-data/{file}'
writeToS3(data=df, bucket_name='golfdfs', filename='data.csv',
          bucket_folder=BUCKET_FOLDER)
print(file, 'data upload complete')
