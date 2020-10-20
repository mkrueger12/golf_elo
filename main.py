from futures3.thread import ThreadPoolExecutor
import time
import pandas as pd
from src.data.data_collection import s3readcsv, get_field, sg_data, writeToS3
from src.features.feature_creation import Elo, addPlayerToLeague, playerRoundSim

# tournament info
sims = 3
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
t1 = time.time()

if __name__ == '__main__':
    with ThreadPoolExecutor() as executor:
        for iteration in range(1, (sims + 1)):
            results = []
            print('Sim Tournament:', iteration)
            all.append(executor.submit(playerRoundSim, sg, field, eloLeague, elo_collect, iteration, results))
    print('Executing Elo')

t2 = time.time()

updated = []
for player in field:
    dict = {'player': player,
            'elo': eloLeague.ratingDict[player]}
    updated.append(dict)

df = pd.DataFrame.from_dict(updated)

print((t2-t1)/60/60)

df.sort_values('elo', inplace=True, ascending=False)
df['rank'] = df['elo'].rank(ascending=False).astype(int)
df['tournament'] = tourn_name

# write to s3
file = 'elo-sim'
BUCKET_FOLDER = f'raw-data/{file}'
writeToS3(data=df, bucket_name='golfdfs', filename='data.csv',
          bucket_folder=BUCKET_FOLDER)
print(file, 'data upload complete')
