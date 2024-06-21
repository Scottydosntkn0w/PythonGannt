import sys
import pandas as pd
import numpy as np
import pymsgbox
from datetime import datetime
import math
from datetime import timedelta

dayName = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")

# Data validation using ymsgbox
def alertSheet(ws):
    """ ws is worksheet name.
        Return message box if ws name is not as required."""
    #Required Worksheet is existing?
    pymsgbox.alert(text= "Worksheet " + ws + " Does not exist", title= 'worksheet' + ws)
    sys.exit()

def alertMatch(tobeMatched, base, atitle):
    #Compare a list to another list. This is used to ensure that worksheet columns index are correcty described
    unmatch = np.setdiff1d(tobeMatched, base)
    # unmatch = [item for item in tobeMatched if item not in base]
    if len(unmatch) > 0:
        unmatch = ', '.join([str(x) for x in unmatch])
        pymsgbox.alert(text= unmatch + ' are to be corrected.', title = atitle)
        sys.exit()

def alertDuplicate(sr):
    #sr should consit of elements which must be unique to each other
    dups = sr[sr.duplicated()]
    if len(dups) > 0:
        dups = ', '.join([str(x) for x in dups])
        pymsgbox.alert(text= dups + ' are duplicated.', title = 'To be unique')
        sys.exit()
def alertPeriodBlank(df):
    #severy activity should be defined with period
    acts = [df['Activity'][i] for i in range(len(df['Activity'])) if df['Period'].isnull()[i] == True] # == ['nan]] #['nan'] ]
    if len(acts) > 0:
        acts = ', '.join([str(x) for x in acts])
        pymsgbox.alert(text= acts + ' period is blank.', title = 'Activites with blank period')
        sys.exit()
def alertDataEntry(df):
    # Filter activities without predecessors and with unspecified start dates
    filtered_activities = df[(df['Predecessor'].isnull()) & (df['Start'].isnull())]
    
    if not filtered_activities.empty:
        # Join the activities into a comma-separated string
        activities_str = ', '.join(filtered_activities['Activity'])
        
        # Display alert
        pymsgbox.alert(text=f"Start dates are not specified for the following activities without predecessors involved: {activities_str}", 
                       title="Activities without predecessors")
        sys.exit()

def remove_space(list):
    """Assumes list includes nested sublist
        Returns 2D list without white space for each element"""
    for i in range(len(list)):
        for j in range(len(list[i])):
            list[i][j] = list[i][j].strip()
    return list

def match_pred(preds, match, option):
    """Assume preds is predecessors list, match is the list to be matched with predecessor
        option is either 'Relationship' or 'Time lag'
        return matching list corresponding to predecessor"""
    match_pred = []
    for i in range(len(preds)):
        if preds[i] != 'nan':
            cols = []
            if len(preds[i]) == len(match[i]):
                for j in range(len(preds[i])):
                    if match[i][j] == 'nan':
                    #if match[i][j].isnull() == True: # 'nan':
                        if option == 'Relationship':
                            cols.append('fs')
                        else:
                            cols.append(0)
                    else:
                        cols.append(match[i][j])
                match_pred.append(cols)
            else:
                for j in range(len(preds[i])):
                    if option == 'Relationship':
                        cols.append('fs')
                    else:
                        cols.append(0)
                match_pred.append(cols)
    return match_pred

def endDate(start, period, holidays, nonworkdays, included):
    """Assumes start is the cmmencing date for the end date calculation, period interger
        holidays the list of project holidays, nonworkdays the list of non working weekdays.
        included being boolean whether start date itself is included as part of period.
        Return start date (working day) and end date"""
    if period < 0: # define whether incremental of decremental
        d = -1
    else:
        d = 1
    period = abs(period)
    if included:
        # Actual start date should be weekday, no weekend date or not holidays
        while dayName[start.weekday()] in nonworkdays or start in holidays:
            start += timedelta(days = d)
        # Calculate end date
        endDate = start
        if period == 0 or period == 1:
            return start, endDate
        else:
            i= 1
            while i < period:
                endDate += timedelta(days = d)
                if dayName[endDate.weekday()] not in nonworkdays and endDate not in holidays:
                    i += 1
            return start, endDate
    else:
        # compute the ending working date with the start date excluded from period
        # This is used to evealuate to the activity start date from the predecessors finish dates for forward pass analysis
        # activity end date from the successors' start date for backward anlaysis, and time lag
        endDate = start
        if period == 0:
            return start, endDate
        else:
            i = 0 
            while i < period:
                endDate += timedelta(days = d)
                # endDate = endDate + timedelta(days = d)
                if dayName[endDate.weekday()] not in nonworkdays and endDate not in holidays:
                    i += 1
            return start, endDate

def pslist(activities, predecessors, periods, relations, lags):
    """
    Assumes activites is the list of activites, predecessors the list of the activity predecessors
    periods the list of activbity periods, relations the list of activity relationship,
    lags the list of activity time lags
    Return the list of every predecessor-successor along with FS equivalent time lag to faclitate
    Return the list of every predecessore-seccessor along with FS equivalent time lag to factlitate
    forward and baward pass calculation, predecessor and successor's corresponding activity indes used for connector
    """
    ps = pd.DataFrame(columns= ['Predecessor','Successor', 'Pred duration','Succ duration','Timelag fs','relation', 'lag','predid', 'succid'])
    n = 0
    for i in range(len(activities)):
        if predecessors[i] != ['None']:
            for j in range(len(predecessors[i])):
                pred = predecessors[i][j]
                succ = activities[i]
                if pred == succ:
                    pymsgbox.alert(text= 'Activity, ' + succ + ' should not take itself as its predecessor, ' + pred, title='Prededecessor')
                    sys.exit()
                predPeriod = periods.loc[activities == pred].values[0]
                succPeriod = periods[i]
                relation = relations[i][j]
                lag = int(float(lags[i][j]))
                predId = activities.loc[activities == pred].index.values[0]
                succId = i
                if predId > succId:
                    pymsgbox.alert(text= 'Activity, ' + pred + 'needs to be placed vefore ' + succ, title = 'Activity Sequence')
                    sys.exit()
                if relation == 'fs':
                    lagfs = lag
                elif relation == 'ss':
                    lagfs = (lag - predPeriod)
                elif relation == 'ff':
                    lagfs = lag=succPeriod
                elif relation == 'sf':
                    lagfs = (lag-predPeriod + succPeriod)
                else:
                    lagfs = lag
                ps.loc[n] = [pred, succ, predPeriod, succPeriod, int(lagfs), relation, lag, predId, succId]
                n += 1
    return ps    