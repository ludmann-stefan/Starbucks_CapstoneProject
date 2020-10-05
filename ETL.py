import numpy as np
import pandas as pd
import math
import json
from matplotlib import pyplot as plt
import seaborn as sns

from datetime import datetime


import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.ticker import LinearLocator, FormatStrFormatter
from mpl_toolkits.mplot3d import Axes3D
from kmodes.kprototypes import KPrototypes



from scipy import interpolate

import sqlite3
from sqlalchemy import create_engine

engine = create_engine('sqlite:///data/data.db')



# read in the json files
portfolio = pd.read_json('data/portfolio.json', orient='records', lines=True)
profile = pd.read_json('data/profile.json', orient='records', lines=True)
transcript = pd.read_json('data/transcript.json', orient='records', lines=True)

transcript.rename (columns = {'time': 'time in hours'}, inplace = True)

def interquartile_range (data, cols, x):

    '''
        In Data Science Interquartile range is defined as the area shown below
        upper = 75 % + (75 % - 25 %)
        lower = 25 % - (75 % - 25 %)

        values outside this range are propably false and do influence the analyzes in a wrong direction
    '''

    for column in cols:
        upper = data.describe () [column]['75%'] + ((data.describe () [column]['75%'] - data.describe () [column]['25%'])*x)
        lower = data.describe () [column]['25%'] - ((data.describe () [column]['75%'] - data.describe () [column]['25%'])*x)
        data = data [data [column] < upper]
        data = data [data [column] > lower]
    return data


def ETL_portfolio ():

    '''
        ETL Part of the Portfolio DF:

        - Duration is in days, matching the transcripts hours
        - the channel col is kind of a dictionary, so I get the dummie Variables.

    '''


    portfolio ['durationHOURS']= portfolio['duration']*24

    i = 0

    channel_dummies = []
    channel_dummies = pd.DataFrame(channel_dummies)
    while i < len (portfolio):
        channel_dummies = channel_dummies.append (pd.DataFrame(pd.get_dummies(portfolio ['channels'].loc [i]).sum (axis = 0)).T, sort = False)
        i += 1
    channel_dummies = channel_dummies.fillna (0).astype (int).reset_index ().drop (['index'], axis = 1)
    channel_dummies


    portfolio_a = portfolio.merge (channel_dummies, left_index = True, right_index = True)
    portfolio_a

    return portfolio_a



def get_Transcript_values (start, end):
    '''
        the transcript value col is also a dictionary
        for faster access it has to be rewritten

    '''

    value = []
    amount = []


    i = start
    while i < end:
        try:
            len (transcript['value'].loc[i].get ('offer id'))> 1
            if len (transcript['value'].loc[i].get ('offer id'))> 1:
                value.append (transcript['value'].loc[i].get ('offer id'))
                amount.append (0)

            elif len (transcript['value'].loc[i].get ('offer_id'))> 1:
                value.append (transcript['value'].loc[i].get ('offer_id'))
                amount.append (0)

            elif len (transcript['value'].loc[i].get ('amount'))> 1:
                value.append (0)
                amount.append (transcript['value'].loc[i].get ('amount'))
            else:
                value.append (0)
                amount.append (0)
        except:
            if (transcript['value'].loc[i].get ('amount')) is not None:
                value.append (0)
                amount.append (transcript['value'].loc[i].get ('amount'))

            elif len (transcript['value'].loc[i].get ('offer_id'))> 1:
                value.append (transcript['value'].loc[i].get ('offer_id'))
                amount.append (0)

            else:
                value.append (99999)
                amount.append (99999)


        i += 1



    value = pd.DataFrame (value)
    values = pd.get_dummies (value[0]).merge (value, left_index = True, right_index = True)
    amount = pd.DataFrame (amount)
    transcript_a = transcript.merge (amount, left_index = True, right_index = True)
    transcript_a = transcript_a.merge (values, left_index =True, right_index = True)
    transcript_a.rename (columns = {'0_y': 'offer_id', 0: 'amount'}, inplace = True)

    return transcript_a




def ETL_transcript (transcript):

    '''
        I do find it helpfull to also have the expiratioin time
        and the offer type within that data frame.
    '''



    expiration = []
    offer_type = []
    i = 0

    while i < len(transcript):

        if transcript.iloc [i] ['event'] == 'offer received':
            expiration.append (transcript.iloc [i] ['time in hours'] + int(portfolio [portfolio ['id'] == transcript.iloc [i] ['offer_id']]['durationHOURS']))
            offer_type.append (portfolio.set_index (['id']).loc [transcript.iloc [i] ['offer_id']] ['offer_type'])
        else:
            expiration.append (transcript.iloc [i] ['time in hours'])
            offer_type.append ('0')


        i += 1

    transcript ['expiration'] = expiration
    transcript ['offer_type'] = offer_type

    return transcript

def get_visit_data (transcript):

    '''
        Not quite sure if i can get a detailled user profil.
        Human behavior can be shown between weeks.
        I am therefor setting up a day dummie variable and a week variable

        be aware, that it is unclear wether day 0 of the week is a monday or maybe a thursday.
        But all days labeled day-0 are the same day in the week.
    '''

    day_of_month = (transcript['time in hours']/24).astype (int)
    day_of_month = pd.DataFrame (day_of_month)


    day_of_week = day_of_month - 7*(day_of_month / 7).astype (int)
    day_of_week = pd.DataFrame (day_of_week['time in hours'])

    week_of_month = (day_of_month / 7).astype (int)


    day_of_month.rename (columns = {'time in hours': 'DayOfMonth'}, inplace = True)
    day_of_week.rename (columns = {'time in hours' : 'DayOfWeek'}, inplace = True)
    week_of_month.rename (columns = {'time in hours' : 'WeekOfMonth'}, inplace = True)

    transcript = transcript.merge (day_of_week, left_index = True, right_index = True)
    transcript = transcript.merge (week_of_month, left_index = True, right_index = True)
    transcript.rename (columns = {'0_x': 'count', '0_y': 'offer_id'}, inplace = True)
    transcript = transcript.merge (day_of_month, left_index = True, right_index = True)


    return transcript

def split_on_event (data):

    '''
        Spliting the data set for better performance

    '''

    received = data [data ['event'] == 'offer received']

    viewed = data [data ['event'] == 'offer viewed']

    completed = data [data ['event'] == 'offer completed']

    transactions = data [data ['event'] == 'transaction']

    return received, viewed, completed, transactions


def ETL_profiles (profile):
    '''
        drop Nan and none gender, aswell as age == 118

    '''
    profile = profile.dropna (axis = 0, subset =  ['income'])
    genders = pd.get_dummies (profile ['gender'])
    profile = profile [profile ['age'] < 100]
    profile = profile.dropna (axis = 0, subset = ['income'])


    '''
        Member since is a date, but I rewrite it in weeks till the very moment.
    '''

    profiles_b = profile

    i = 0
    member_since = []

    while i < len (profiles_b):
        member_since.append (int(int(str(datetime.today () - datetime (int(profiles_b.iloc [i]['became_member_on'].astype (str)[0:4]),
                              int (profiles_b.iloc [i]['became_member_on'].astype (str)[4:6]),
                              int (profiles_b.iloc [i]['became_member_on'].astype (str)[6:]))).split (' ')[0])/ 7))
        i += 1

    genders = genders.merge (pd.DataFrame (member_since), left_index = True, right_index = True)

    profile = profile.merge (genders, left_index = True, right_index = True)
    profile.rename (columns = {0: 'member_x_weeks'}, inplace = True)

    return (profile)


'''
________________________________________________________________________________

'''

portfolio = ETL_portfolio()

print ('ETL Portfolio')


transcript = get_Transcript_values (0, len (transcript))
transcript = interquartile_range (transcript, ['amount'], 3)
transcript = ETL_transcript (transcript)
transcript = get_visit_data(transcript)


print ('ETL Transcript')

received, viewed, completed, transactions = split_on_event (transcript)


profile = ETL_profiles (profile)
print ('ETL Profiles')


'''
________________________________________________________________________________
'''

def get_user_interactions (user_id):
    user_int = transcript [transcript ['person'] == user_id]
    return user_int

def get_offer_response (i):

    # data = 3 Parts... + transactions
    received_index = i
    received_matrix = received.loc [i]
    offer_id = (received.loc [i] ['offer_id'])
    person = (received.loc [i] ['person'])
    received_time = received.loc [i] ['time in hours']
    expiration_time = received.loc [i] ['expiration']

    viewed_yn = []
    viewtoexpire = []
    completed_yn = []
    completed_yn_unknown = []
    compacttime = []
    comp_matrix = []
    liste = []



    view_matrix = viewed [viewed ['person'] == person][viewed [viewed ['person'] == person]['offer_id'] == offer_id]
    view_matrix = view_matrix [view_matrix ['time in hours'] >= received_time]
    view_matrix = view_matrix [view_matrix ['time in hours'] <= expiration_time]

    try:
        viewtime = view_matrix.iloc [0]['time in hours']
    except:
        viewtime = -98


    comp_matrix = completed [completed ['person'] == person][completed [completed ['person'] == person]['offer_id']== offer_id]
    comp_matrix = comp_matrix [comp_matrix ['time in hours'] >= received_time]

    try:
        completed_time = (comp_matrix.iloc [0]['time in hours'])
    except:
        completed_time = -99



    if viewtime >= 0:
        viewed_yn = 1
        viewtoexpire = expiration_time - viewtime
    else:
        viewed_yn = 0
        viewtoexpire = 0

    if completed_time >= 0:
        if viewtime >= 0:
            compacttime =  completed_time - viewtime
            completed_yn = 1
            completed_yn_unknown = 0
        else:
            compacttime = 0
            completed_yn_unknown =  1
            completed_yn = 0
    else:
        completed_yn_unknown = 0
        compacttime = 0
        completed_yn = 0


    index_received = received_index
    liste = index_received

    index_viewed = -1
    try:
        index_viewed = view_matrix.index [0].astype (int)
        liste.append (index_viewed)
    except: index_viewed = index_viewed

    index_completed = -1
    try:
        '''
            transaction Data, that leads into a completed offer is one record above!!
        '''
        index_completed = (comp_matrix.index [0].astype (int) - 1)

        if (transcript.loc [index_completed]['event'] == 'transaction'):
            index_completed = index_completed

        elif (transcript.loc [index_completed -1 ]['event'] == 'transaction'):
            index_completed = index_completed -1

        else:
            index_completed = -1

        liste.append (index_completed)
    except: index_completed = index_completed


    start = viewtime
    end = min (completed_time, expiration_time)


    visits = transactions [transactions ['person'] == person][transactions [transactions ['person'] == person] ['time in hours'] >= start]
    visits = visits [visits ['time in hours'] < end]
    visits_count = len (visits [visits ['time in hours'] < end])



    vector = ['offer_id', 'person', 'viewed_yn', 'viewtoexpire', 'completed_yn', 'compacttime', 'completed_yn_unknown', 'index_received', 'index_viewed', 'index_completed', 'visits_count']

    offer_response_matrix = pd.DataFrame ([offer_id, person, viewed_yn, viewtoexpire, completed_yn, compacttime, completed_yn_unknown, index_received, index_viewed, index_completed, visits_count])

    return offer_response_matrix


def get_user_transaction_data ():

    '''
        kind of a customer analyses, but keep in mind, that there is no split between control and test group.
        additional, since there is no control period, this data is distracted
    '''

    transcript_test = transcript

    visits_person = transactions.groupby (['person'])['amount'].value_counts().unstack().sum (axis = 1)

    frequent_visits = []
    frequent_visits = pd.DataFrame (frequent_visits)


    total_spent_each_person = pd.DataFrame(transcript_test [transcript_test ['event'] == 'transaction'].groupby (['person']).sum ())
    frequent_visits = frequent_visits.merge (total_spent_each_person, left_index = True, right_index = True, how = 'outer')



    value_person = transcript_test.groupby (['person'])['amount'].sum ().sort_values(ascending = False)
    deep_value_person = transcript_test [transcript_test ['event'] == 'transaction'].groupby (['person'])['amount'].describe ()
    i = 0
    std_arr = []


    while i < len (deep_value_person):
        try: std_arr.append (int(deep_value_person['std'].iloc[i]*100)/100)
        except: std_arr.append (-1)
        i+= 1

    deep_value_person = deep_value_person.reset_index ().merge (pd.DataFrame (std_arr), left_index = True, right_index = True).set_index (['person'])
    deep_value_person = deep_value_person.drop(['std'], axis = 1).rename (columns = {0: 'std'})


    profile_return = profile.merge (deep_value_person, left_on = 'id', right_on = 'person')

    return profile_return


# get the matrix (viewtime, ...) on each offer!

def get_reaction_matrix (list_of_customers):


    a = 0
    # count variable

    matrix = []
    matrix = pd.DataFrame (matrix)

    while a <  len (list_of_customers):
        '''
        If you want a smaller set of customers
        '''

        person = list_of_customers [a]
        offers = get_user_interactions (person) [get_user_interactions (person) ['event'] == 'offer received']
        i = 0
        while i < len (offers):
            n = offers.index [i]
            matrix = matrix.append (get_offer_response (n).T)
            i += 1
        a += 1


    vector = ['offer_id', 'person', 'viewed_yn', 'viewtoexpire', 'completed_yn', 'compacttime', 'completed_yn_unknown', 'index_received', 'index_viewed', 'index_completed', 'visits_count']

    matrix.columns = pd.DataFrame(vector).T.iloc [0]



    received_reaction_matrix = received.merge (matrix, left_index = True, right_on = 'index_received')


    matrix.columns = pd.DataFrame(vector).T.iloc [0]

    received_reaction_matrix = received.merge (matrix, left_index = True, right_on = 'index_received')
    return received_reaction_matrix

def store_df (data, name):

    '''
        store data function in SQL
    '''

    name_in = name

    data.to_sql (name_in, engine, if_exists = 'replace')
    print (name_in, ' stored')
    return



'''
________________________________________________________________________________
'''

profile = get_user_transaction_data ()
print ('ETL offer response Matrix')


list_of_customers = (list(profile ['id']))

received_reaction_matrix = get_reaction_matrix (list_of_customers)
received_reaction_matrix

visits_person = transactions.groupby (['person'])['amount'].value_counts().unstack().sum (axis = 1)


profiles_in_use = interquartile_range (profile, ['age', 'income', 'max'], 1)
profiles_in_use
print ('Profiles are cleaned')

'''
    time to store all data

'''

### portfolio:
name = 'portfolio'
store_df (portfolio.drop (['channels'], axis = 1), name)

### profile:
name = 'profiles'
store_df (profiles_in_use, name)

### transcript:
name = 'transcript'
store_df (transcript.drop (['value'], axis = 1).astype (str), name)


### received-reaction-matrix:
name = 'received_reaction_matrix'
store_df (received_reaction_matrix.drop (['value'], axis = 1).astype (str), name)
