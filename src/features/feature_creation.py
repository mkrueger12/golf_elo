import pandas as pd
from itertools import combinations
from scipy.stats import skewnorm

class Elo:

    def __init__(self, k, g=1):
        self.ratingDict = {}
        self.k = k
        self.g = g

    def addPlayer(self, name, rating=1500):
        self.ratingDict[name] = rating

    def gameOver(self, winner, loser):

        result = self.expectResult(self.ratingDict[winner], self.ratingDict[loser])

        self.ratingDict[winner] = self.ratingDict[winner] + (self.k * self.g) * (1 - result)
        self.ratingDict[loser] = self.ratingDict[loser] + (self.k * self.g) * (0 - (1 - result))

    def expectResult(self, p1, p2):
        exp = (p2 - p1) / 400.0
        return 1 / ((10.0 ** (exp)) + 1)


def addPlayerToLeague(field, elo_initial, eloLeague):
    """ Takes a list of players in a given tournament field
        and adds them to the eloLeague """

    for player in field:

        if player in elo_initial:
            rate = pd.DataFrame(elo_initial[elo_initial['player'] == player]['elo'])
            eloLeague.addPlayer(player, rating=rate.iloc[0, 0])
            print(player, rate)
        else:
            eloLeague.addPlayer(player, rating=800)
            print(player, '800')



def createCombos(combos, sg_sims, eloLeague, iteration):

    """ Matches each combination of player and decides
        who wins the matchup """

    p1 = combos[0]
    p2 = combos[1]
    p1_score = list(sg_sims[sg_sims['name'] == p1]['sg'])
    p2_score = list(sg_sims[sg_sims['name'] == p2]['sg'])

    if p1_score > p2_score:
        eloLeague.gameOver(winner=p1, loser=p2)
    else:
        eloLeague.gameOver(winner=p2, loser=p1)

    dict = {'name1': p1,
            'elo1': eloLeague.ratingDict[p1],
            'score1': p1_score,
            'name2': p2,
            'elo2': eloLeague.ratingDict[p2],
            'score2': p2_score,
            'sim_round': iteration}

    return dict


def trn_sim(stroke_data, player, iteration):
    df = stroke_data[stroke_data['full'] == player]

    if len(df) > 99:
        mu, sigma, skew = df['sg:tot'].mean(), df['sg:tot'].std(), df['sg:tot'].skew()  # mean, standard deviation, skew per player
    else:
        mu, sigma, skew = stroke_data['sg:tot'].mean(), stroke_data['sg:tot'].std(), stroke_data['sg:tot'].skew()  # mean, standard deviation, skew of PGA Tour

    score = skewnorm.rvs(skew, loc=mu, scale=sigma, size=4)
    score = pd.DataFrame([score.sum()])
    score.rename(columns={score.columns[0]: 'sg'}, inplace=True)
    score['tournament'] = iteration
    score['name'] = player
    return score


def playerRoundSim(sg, field, eloLeague, elo_collect, iteration, results):
    """ simulates each player rounds matchup and appends results to list """


    plist = []
    for player in field:
        x = trn_sim(sg, player, iteration=iteration)
        results.append(x)
        plist.append(player)

    df = pd.concat(results)

    combos = [c for c in combinations(plist, 2)]

    for c in combos:
        elo_outcome = createCombos(c, df, eloLeague, iteration)

        elo_collect.append(elo_outcome)

    return elo_collect
