import numpy as np
import pandas as pd
import xlwings as xw
from GanttFunciton import *
from datetime import timedelta
from datetime import date
from calendar import monthrange
from xlwings.utils import rgb_to_int

dayName_short = ("M", "T", "W","T","F","S","S")
months = ("Jan","Feb", 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')
chart_startcol = int(12)
chart_startrow = int(4)
chart_columnwidth = 1.0 # unit in point
chart_rowheight = 20 # unit in point and dfault is 15 points

# API options and settings
pd.set_option('expand_frame_repr', False)
pd.options.mode.chained_assignment = None

# Establish connection to Excel workbook using xlwings
# # option 1 - when running python from Excel using caller() method
# wb = xw.Book.caller()
# filename = wb.fullname

#option 2 - when running directly in the Python, giving Excel file name
filename = 'Ganttwoclass.xlsm'
wb = xw.Book(filename)

# Identify sheet name
wsname1 = 'cpa'
wsname2 = 'Calendar'

# check whether  worksheet named 'cpa' exists or not
try:
    ws1 = wb.sheets[wsname1]
except:
    alertSheet(wsname1)
try:
    ws2 = wb.sheets[wsname2]
except:
    alertSheet(wsname2)

# Read Excel dat
# 1) Schedule data, read and store in a pandas DataFrame
#schData = pd.read_excel(io = filename, sheet_name= wsname1, index_col=None, usecols = 'A:K', skiprows = 2,)
schData = ws1.range('A3').options(pd.DataFrame, index=False, header=1, expand='table').value
print(schData)
size = len(schData)
# 2) Calendar data, read and store in a pandas DataFrame
calData = ws2.range('A1').options(pd.DataFrame, index=False, header=True, expand='table').value
print(calData)

# Validate Excel data
# 1) Validate schedule data
colsSch = ['Activity', 'Description', 'Predecessor', 'Period', 'Start', 'Finish', 'Late Start', 'Late Finish', 'Float', 'Relationship', 'Time lag']
userColsSch = schData.columns.tolist() # array converted to list
alertMatch(userColsSch, colsSch, 'Schedule data columns unmatch') # check whether spread sheet columns are correctlyu described as coded in program
alertDuplicate(schData['Activity']) # Activiteies defined are not to be duplicated
alertPeriodBlank(schData[['Activity','Period']])# Activites without periods found
alertDataEntry(schData[['Activity','Predecessor', 'Start']]) # alertBlank(schData['redecessor'], schData['Start'])
# 2) Validate calendar data
colsCal = ['type', 'Date', 'Hours', 'Note']
userColsCal = calData.columns.tolist()
alertMatch(userColsCal, colsCal, 'Project calendar data columns unmatch')

# data transformation
# (1) convert Excel cell contents (desinged for multiple elements and separate by comma) into a list: applicable to predecessors, relationships and time lags
# (1-1) split
# Using for in loop
# predecessors = []
# for preds in schData['Predecessor'].apply(str):
#   pred = preds.split(',')
#   predecessors.append(pred)
#Based on list comprehension
predecessors = [preds.split(',') for preds in schData['Predecessor'].apply(str)]

#(1-2) remove space
# Based on a custom function (remove_space with iterations)
# predecessors = remove_space(predecessors)
# Based on nested list comprehesion
predecessors = [[x.strip() for x in [preds.split(',') for preds in schData['Predecessor'].apply(str)][i]] for i in range(size)]
relations = [[x.strip() for x in [rels.split(',') for rels in schData['Relationship'].apply(str)][i]] for i in range(size)]
lags = [[x.strip() for x in [timelags.split(',') for timelags in schData['Time lag'].apply(str)][i]] for i in range(size)]

# (1-3) Realationship and time lag data to match with predecessors
relations = match_pred(predecessors, relations ,'Relationship')
lags = match_pred(predecessors, lags, 'Time lag')

#(2) Sort out Holidays and non work days into corresponding list
holidays = calData['Date'].loc[calData['type'] == 'Holiday'].values
nonworkdays = calData['Note'].loc[(calData['type'] == 'Weekday') & (calData['Hours'] == 0)].values

#(3) Create predecessor - seccessor list, each one to one relation
# Create the list of 


ps = pslist(schData['Activity'], predecessors, schData['Period'], relations, lags)
# predecessors should refer to defined activity only
#alertReference(ps['Predecessor'], schData['Activity'])

# (4) Find out project start activity and project end activity
p = set(ps['Predecessor'])
s = set(ps['Successor'])
startActs = p - s
endActs = s - p

# forward pass analysis for early start and early finish date
# # with nested loop
# for x in startActs:
#     for i in range(len(schData)):
#         if x == schData['Activity'][i]:
#             earlySch=endDate(schData['Start'][i], schData['Period'][i], holidays, nonworkdays, included=True)
#             schData['Start'][i] = earlySch[0]
#             schData['Finish'][i] = earlySch[1]
#             # print(type(schData['Finish']))
# withou nested loop
for x in startActs:
    i = schData['Activity'].loc[schData['Activity'] == x].index.values[0]
    earlySch = endDate(schData['Start'][i], schData['Period'][i], holidays, nonworkdays, included=True)
    schData['Start'][i] = earlySch[0]
    schData['Finish'][i] = earlySch[1]
for i in range(len(schData)):
    if predecessors[i] != ['nan']:
        preds = predecessors[i]
        predsEnds = []
        for j in range(len(preds)):
            predsLag = ps['Time lag fs'].loc[(ps['Predecessor'] == preds[j]) & (ps['Successor'] == schData['Activity'][i])].values[0]
            predsEnd = pd.Timestamp(schData['Finish'].loc[schData['Activity'] == preds[j]].values[0])
            predsEnd.append(endDate(predsEnd, predsLag, holidays, nonworkdays, False)[1])
        schData['Start'][i] = endDate(max(predsEnd), 1, holidays, nonworkdays, False)[1]
        schData['Finish'][i] = endDate(schData['Start'][i], schData['Period'][i], holidays, nonworkdays, True)[1]

# transverse list in the reverse order
# # method 1 based on list slicing
# for act in schData['Activity'][::-1]:
# # method 2 based on the range(start, stop, step) functio
for i in range(len(schData)-1, -1, -1):
    # Find out the activity's successor(s):
    succs = ps['Successor'].loc[ps['Predecessor'] == schData['Activity'][i]].tolist()
    if len(succs) != 0:
        succStarts = []
        for j in range(len(succs)):
            succLag = ps['Time lag fs'].loc[(ps['Successor'] == succs[j]) & (ps['Predecessor'] == schData['Activity'][i])].values[0]
            succStart = pd.Timestamp(schData['Late Start'].loc[schData['Activity'] == succs[j]].values[0])
            succStarts.append(endDate(succStart, -succLag, holidays, nonworkdays, False)[1])
        schData['Late Finish'][i] = endDate(min(succStarts), -1, holidays, nonworkdays, False)[1]
        schData['Late Start'][i] = endDate(schData['Late Finish'][i],-schData['Period'][i], holidays, nonworkdays, True)[1]

# Float calculation
schData['Float'] = schData['Late Finish'] - schData['Finish']

# Complte Schedule data based on Critical path analysis result
schData['Start'] = [x.date() for x in schData['Start']]
schData['Finish'] = [x.date() for x in schData['Finish']]
schData['Late Start'] = [x.date() for x in schData['Late Start']]
schData['Late Finish'] = [x.date() for x in schData['Late Finish']]
schData['Float'] = [x.date() for x in schData['Float']] # time delta is to be coverted to days

# write to the worksheet, the schData DataFrame with analysis done above
calculatedData = pd.DataFrame(schData, columns=['Start', 'Finish', 'Late Start','Late Finish', 'Float'])
wb.sheets[wsname1].range('E3').options(index=False).value = calculatedData # write to worksheet

# Three(3) tiers schedule timeplate consisting of year + month, date, weekdays
# (1) clean the existing chart area from chart starting column 12 to the existing last column including deleting shapes (bars and connectores)
last_column = ws1.range(3,1).end('right').last_cell.column
ws1[:, chart_startcol-1:last_column + 1].delete()
ws1[chart_startrow-1:chart_startrow + size, :].api.RowHeight = chart_rowheight
ws1[chart_startrow-1:chart_startrow + size, :].api.VerticalAlignment = xw.constants.VAlign.xlVAlignCenter #vertical alignment middle

# (2) three(3) tier template data (covering from 1st day of the starting month till end of the ending month)
dayone = min(schData['Start'])
dayend = max(schData['Late Finish'])
dayone = date(dayone.year, dayone.month, 1)
dayend = date(dayend.year, dayend.month+1, 1) - timedelta(days=1)
top = [months[(dayone + timedelta(days = i)).month-1] + '-' + str((dayone + timedelta(days=i)).year) for i in range((dayend - dayone).days + 1)] # month - year
mid = [dayName_short[(dayone + timedelta(days = i)).weekday()] for i in range((dayend - dayone).days + 1)] # weekday in short
bottom = [(dayone + timedelta(days = i)).day for i in range((dayend-dayone).days + 1)]# date
templateData = pd.DataFrame(list(zip(top,mid,bottom))).transpose()
ws1.range('L1').options(index=False, header = False).value = templateData
# (3) top layer format requiring merge cells extending each associated onths's day
month_years = (dayend.month - dayone.month + 1) +12*(dayend.year - dayone.year)
colmerges = [monthrange(dayone.year, dayone.month + i) [i] for i in range(month_years)]
for i in range(len(colmerges)):
    if i == 0:
        mstart = chart_startcol
        mend = mstart +colmerges[i] - 1
        ws1.range((1, mstart), (1, mend)).merge()
    else:
        mstart = mend + 1
        mend = mstart + colmerges[i] - 1
        ws1.range((1, mstart), (1, mend)).merge()

# (4) Highlight nonworkdays
holidays = [x.date() for x in holidays]
holCols = [i + chart_startcol for i in range((dayend - dayone).days) if dayone +timedelta(days = i) in holidays]
nonworkdays = [i + chart_startcol for i in range((dayend - dayone).days) if dayone + timedelta(days = i) in holidays or dayName[(dayone + timedelta(days=i)).weekday()] in nonworkdays]

for i in range(len(nonworkdays)):
    ws1.range(3, nonworkdays[i]).color = (230,175,220)

# create schedule barcharts with relationship connectors
# (1) early schedule bars
startcol = (schData['Start'] - dayone).dt.days + chart_startcol # schData['Start'] is series, schdata['start'] - dayone is series
chart_columnwidth_point = ws1.cells(chart_startrow,chart_startcol).width
barlength = ((schData['Finish'] - schData['Start']).dt.days +1) * chart_columnwidth_point - 0.2 * chart_columnwidth_point # number of columns per period
bars = [None] * len(schData) # Store each shapes instance for drawing connectors
for i in range (len(schData)):
    left = ws1.range(chart_startrow + i, startcol[i]).left + 0.1 * chart_columnwidth
    top = ws1.range(chart_startrow + i, startcol[i]).top + 0.1 * chart_rowheight
    width = barlength[i]
    height = chart_rowheight*0.35
    bars[i] = ws1.api.Shapes.AddShape(1, left, top, width, height)
    if schData['Float'][i] == 0:
        bars[i].Fill.ForeColor.RGB = rgb_to_int((255,51,204))
    else:
        bars[i].Fill.ForeColor.RGB = rgb_to_int((80,58,244))

# (2) produce connectors to show schedule activitties relationship based on early schedule bars
for i in range(len(ps)):
    ipred = ps['predid'][i]
    isucc = ps['succid'][i]
    if ipred != 'None':
        if ps['relation'][i] == 'fs':
            ibegin = 4
            iend = 2
        elif ps['relation'][i] == 'ss':
            ibegin = 2
            iend = 2           
        elif ps['relation'][i] == 'ff':
            ibegin = 4
            iend = 4  
        elif ps['relation'][i] == 'sf':
            ibegin = 2
            iend = 4  
        connector = ws1.api.Shapes.AddConnector(2, left, top, 1,1) # connector type 2 for elbow connector
        connector.Line.EndArrowheadStyle = 4 # end arrow head style 4 for stealth bomber
        connector.Line.ForeColor.RGB = rgb_to_int((0,0,0))
        connector.ConnectorFormat.BeginConnect(bars[ipred], ibegin)
        connector.ConnectorFormat.EndConnect(bars[isucc], iend)

# (3) Late schedule bars
startcol_late = (schData['Late Start'] - dayone).dt.days + chart_startcol
barlength_late = ((schData['Late Finish'] - schData['Late Start']).dt.days + 1) * chart_columnwidth_point - 0.2 * chart_columnwidth_point
for i in range(len(schData)):
    left = ws1.range(chart_startrow + i, startcol_late[i]).left + 0.1 * chart_columnwidth
    top = ws1.range(chart_startrow + i, startcol_late[i]).top + 0.55 * chart_rowheight
    width = barlength_late[i]
    height = chart_rowheight*0.35
    bar_late = ws1.api.Shapes.AddShape(1, left, top, width, height)
    bar_late.Fill.ForeColor.RGB = rgb_to_int((175,175,175))
    bar_late.ZOrder(1) #ZOrdercmd is parameter, 1 for SendtoBack, this is for the purpose of connectoers not ito be below bars

# schedule chart area formatting - column width, borders (thin and light grey), orientation 90 degree
last_column = ws1.range(3,1).end('right').last_cell.column # check last column number for very inital pgoram run
ws1.range((chart_startrow, chart_startcol), (chart_startrow + len(schData), (dayend-dayone).days + chart_startcol)).column_width = chart_columnwidth
ws1.range((1, chart_startcol), (chart_startrow + len(schData) -1, last_column)).api.Borders.Weight = 2 # thin
ws1.range((1, chart_startcol), (chart_startrow + len(schData) -1, last_column)).api.Borders.Color = rgb_to_int((166,166,166)) # light grey


