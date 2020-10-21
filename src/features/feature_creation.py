import pandas as pd
from itertools import combinations
from scipy.stats import skewnorm
import numpy as np

from numba import jit
import boto3
from io import StringIO

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


def addPlayerToLeague(field, elo_initial, eloLeague, plist):
    """ Takes a list of players in a given tournament field
        and adds them to the eloLeague """

    for player in field:

        if player in plist:
            rate = pd.DataFrame(elo_initial[elo_initial['player'] == player]['elo'])
            eloLeague.addPlayer(player, rating=rate.iloc[0, 0])
            print(player, rate)
        else:
            eloLeague.addPlayer(player, rating=955)
            print(player, '955')



def createCombos(sg_sims, eloLeague, iteration, combos):

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

# @jit(nopython=True) # Set "nopython" mode for best performance, equivalent to @njit
def trn_sim(stroke_data, player, iteration):
    df = stroke_data[stroke_data['full'] == player]

    if len(df) > 99:
        mu, sigma, skew = df['sg:tot'].mean(), df['sg:tot'].std(), df['sg:tot'].skew()  # mean, standard deviation, skew per player
    else:
        mu, sigma, skew = stroke_data['sg:tot'].mean(), stroke_data['sg:tot'].std(), stroke_data['sg:tot'].skew()  # mean, standard deviation, skew of PGA Tour

    score = np.array([skewnorm.rvs(skew, loc=mu, scale=sigma, size=4)])
    score = np.array([score.sum()])
    sr = np.full((score.shape[0]), iteration)  # add sim round to nparry
    score = np.hstack((score, sr))
    play = np.full((score.shape[0]), player)  #add player name
    score = np.hstack((score, play))
    return score


def playerRoundSim(sg, field, eloLeague, results, iteration):
    """ simulates each player rounds matchup and appends results to list """
    print(iteration)

    plist = []
    for player in field:
        x = trn_sim(sg, player, iteration=iteration)
        results.append(x)
        plist.append(player)

    df = pd.concat(results)

    combos = [c for c in combinations(plist, 2)]

    cc = lambda x: createCombos(df, eloLeague, iteration, x)

    list(map(cc, combos))

