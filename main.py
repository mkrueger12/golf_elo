from futures3.thread import ThreadPoolExecutor
import time
import pandas as pd
from src.data.data_collection import s3readcsv, get_field, sg_data, writeToS3
from src.features.feature_creation import Elo, addPlayerToLeague, playerRoundSim

# tournament info
year = '2021'
tourn_id = '521'
tour_code = 'r'
sims = 1000
cut_line = 70

# import data
sg = sg_data(date='2017-09-01')

elo_initial = s3readcsv(bucket_name='golfdfs', bucket_folder='raw-data/elo',
                 filename='data_initial.csv')

# download tournament field
try:
    field = get_field(league='PGA', n=3)
    field = list(field['name'].unique())
except KeyError:
    field = elo_initial.sample(n=20)
    field = list(field['player'])

# filter on tournament field
elo_initial = elo_initial[elo_initial['player'].isin(field)]
sg = sg[sg['full'].isin(field)]

# create league
elo_players = list(elo_initial['player'].unique())

eloLeague = Elo(k=32, g=1)  #.2215

# add players in trn to eloLeague
addPlayerToLeague(field=field, elo_initial=elo_initial, eloLeague=eloLeague)

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

print((t2-t1))




