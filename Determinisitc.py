import sys
import pandas as pd
import numpy as np
import xlwings as xw
from datetime import datetime, timedelta, date

class BasicCPA(object):
    """
    Critical path analysis algorithm written in Python
    Assumes working everyday.
    Assumes that relation logic between activities is finish to start (fs) only
    Assumes 0 day time lag from activity's predecessors
    Assumes that project end date for lat schedule is as calcualted for early schedule
    Implements critical path analysis and its outcome on Excel sheet.
    """

    def __init__(self) -> None:
        self.wb = xw.Book('Deterministic.xlsx')
        targetWSName = 'Deterministic'
        try:
            self.targetWs = self.wb.sheets(targetWSName)
        except:
            sys.exit()
        self.targetCell = 'E3' # top left cell of outcome data
        self.floatCol = 9      # Coumn number for float
        self.startCol = int(11)# Gantt chart start column number
        self.startRow = int(4) # Gantt chart start row number
        self.templMonths = int(4) # Schedule template coverage months

    def __cleanUp(self):# Remove existing charts, data tables, and shapes
        self.wb.app.screen_updating = False
        if len(self.targetWs.charts) > 0:
            for _ in range(len(self.targetWs.charts)):
                self.targetWs.charts[0].delete()
        self.targetWs[:, self.startCol-1:37].delete()
        if len(self.targetWs.shapes):
            for sh in self.targetWs.shapes:
                sh.delete()
        self.wb.app.screen_updating = True

    def __readExcel(self):
        try:
            self.wss=self.wb.sheets['Deterministic']
            
        except Exception as e:
            print(f"Error reading Excel file: {e}")
            sys.exit()
        #self.df = pd.read_excel(io = self.wb.fullname, sheet_name = self.wss.name, index_col = None, usecols = 'A:I', skiprows = 2, na_filter = False)
        self.df = self.targetWs.range('A3').options(pd.DataFrame, index=False, header=1, expand='table').value
        print(self.df)
        self.size = len(self.df)
    
    def __validateData(self): # to be detailedi n practical scheduling
        pass
    def __projectCalendar(self):# to be detailed in practical scheduling
        pass

    def __schTemplate(self):
        template = scheduleTemplate(self.targetWs, self.df, self.startCol, self.startRow, self.templMonths)
        self.wb.app.screen_updating = False
        template.timeTable()
        self.wb.app.screen_updating = True
    
    
    
    def startCPM(self):
        """When Gantt chart is displayed,
        the scheudle template is required.
        """
        self.__cleanUp()
        self.__readExcel()
        self.__validateData()
        self.__projectCalendar()
        self.__schTemplate()

    def startCPMwoGantt(self):
        self.__cleanUp()
        self.__readExcel()
        self.__validateData()
        self.__projectCalendar()  

    def projectPeriod(self):
        projectPeriod = self.df['Period']
        return projectPeriod


    def pslist(self):
        self.preds = [str(p).split(',') for p in self.df['Predecesor']]
        ps = [(self.preds[i][j], self.df['Activity'][i]) for i in range(self.size) for j in range(len(self.preds[i]))]
        self.ps = pd.DataFrame(ps, columns=['Predecessor', 'Successor'])
        predID = [[i for i in range(len(self.df['Activity'])) if pred == self.df['Activity'][i]] for pred in self.ps['Predecessor']]
        
        # Flatten 2-D list to 1-D list
        for i in range(len(predID)):
            if len(predID[i]) == 0:
                predID[i] = ''
            else:
                predID[i] = predID[i][0]
        # Flatten 2-D list to 1-D list
        succId = [[i for i in range(len(self.df['Activity'])) if succ == self.df['Activity'][i]] for succ in self.ps['Successor']]
        # Add two columns to dataframe, self.ps
        self.ps['predId'], self.ps['succId'] = predID, succId
    
    def __forwardPass(self):
        # Forward pass analysis for Early start and finish schedule
        for i in range(self.size):
            if self.pred[i] == ['']:
                self.df['Start'][i] = self.df['Start'][i]
                self.df['Finish'][i] = (self.df['Start'][i]) + timedelta(days = int(self.df['Period'][i])-1)
            else:
                preds = self.preds[i]
                predEnds = [pd.Timestamp(self.df['Finish'].loc[self.df['Activity'] == preds[j]].vlaues[0]) for j in range(len(preds))]
                self.df['Start'][i]=(max(predEnds)) + timedelta(days=1)
                self.df['Finish'][i]=(self.df['Start'][i]) + timedelta(days = int(self.df['Period'][i])-1)

    def __backwardPass(self):
        # backward pass analysis for late start and late finish
        # find project end activities and set their start and finish date to the same as ear
        self.endActs = set(self.ps['Successor']) - set(self.ps['PRedecessor'])
        for x in self.endActs:
            for i in range(self.size):
                if x == self.df['Activity'][i]:
                    self.df['Late Start'][i] = self.df['Start'][i]
                    self.df['Late Finish'][i] = self.df['Finish'][i]
        for i in range(self.size-1, -1, -1):
            # find out activity's successor(s)
            succs = self.ps['Successor'].loc[self.ps['Predecessor'] == self.df['Activity'][i]].tolist()
            if len(succs) != 0:
                succStarts = [pd.Timestamp(self.df['Late Start'].loc[self.df['Activity'] == succs[j]].values[0]) for j in range(len(succs))]
                self.df['Late Finish'][i] = (min(succStarts))+timedelta(days = -1)
                self.df['Late Start'][i] = (self.df['Late Finish'][i]) - timedelta(days=int(self.df['Period'][i])-1)
    
    def __float(self): # Float calculation
        self.df['Float'] = self.df['Late Finish'] - self.df['Finish']
        self.df['Float'] = [str(x).split()[0] + ' days' for x in self.df['Float']]
    
    def __cpaOut(self): # Program outcome
        self.calculatedData = pd.DataFrame(self.df, columns=['Activity','Start','Finish','Late Start','Late Finish','Float','Period'])
    
    def __writeExcel(self):
        # write to the worksheet, the self.df DataFrame with analysis done above.
        writeData = self.calculatedData.loc[:,['Start', 'Finish', 'Late Start', 'Late Finish', 'Float', 'Period']]
        self.targetWs.range(self.targetCell).options(index=False).value = writeData
    
    def CPA(self, periods):
        self.df['Period']=periods
        self.pslist()
        self.__forwardPass()
        self.__backwardPass()
        self.__float()
        self.__cpaOut()
        self.__writeExcel()
        self.__Gantt()
        return self.calculatedData
    def CPAwoGantt(self, periods): # Crtical path anysisd only, no Gantt chart
        self.df['Period']=periods
        self.pslist()
        self.__forwardPass()
        self.__backwardPass()
        self.__float()
        self.__cpaOut()
        self.__writeExcel()
        return self.calculatedData


from calendar import monthrange
from xlwings.utils import rgb_to_int
class scheduleTemplate:
    def __init__(self, ws, df, startCol, startRow, templMonths) -> None:
        self.targetWs = ws
        self.df = df
        self.start = min(self.df['Start'].loc[self.df['Predecessor'].isnull()])
        self.start = pd.to_datetime(self.start).date()
        self.end = self.start + timedelta(days = templMonths * 30)
        self.size = len(self.df)
        self.startCol = int(startCol)
        self.startRow = int(startRow)
        self.rowHeight = int(20)
        self.colWidth = float(0.4)
        self.dayName_short = ("M", "T", "W","T","F","S","S")
        self.months = ("Jan","Feb", 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')
        self.borderColor = rgb_to_int((166,166,166))

    def timeTable(self):
        """Three(3) tiers schedule template consisting of year + month, weekday , date"""
        #(1) Remove existing schedule template
        last_column = self.targetWs.range(3,1).end('right').last_cell.column
        try:
            self.targetWs[:, self.startCol-1:last_column +1].delete()
        except:
            pass
        self.targetWs[self.startRow-1:self.startRow + self.size, :].api.RowHeight = self.rowHeight
        self.targetWs[self.startRow-1:self.startRow + self.size, :].api.VerticalAlignment = xw.constants.VAlign.xlVAlignCenter

        #(2) time table data frame and write
        dayOne = date(self.start.year, self.start.month, 1)
        dayEnd = date(self.end.year, self.end.month+1, 1) - timedelta(days=1)
        top = [self.months[(dayOne + timedelta(days = 1)).month-1] + '-' + str((dayOne + timedelta(days = i)).year) for i in range((dayEnd - dayOne).days + 1)] # Year - month
        mid = [self.dayName_short[(dayOne + timedelta(days = i)).weekday()] for i in range((dayEnd - dayOne).days + 1)] # weekday in short
        bottom = [(dayOne + timedelta(days = i)).day for i in range((dayEnd - dayOne).days +1)] # date
        templateData = pd.DataFrame(list(zip(top,mid,bottom))).transpose()
        self.targetWs.range(1, self.startCol).options(index=False, header = False).value = templateData

        #(3) top layer for year month requires cells merging month by month
        monthCount = (dayEnd.month - dayOne.month +1) + 12*(dayEnd.year - dayOne.year)
        yearMonths = [dayOne + pd.DateOffset(months = i) for i in range(monthCount)]
        colmerges = [monthrange(yearMonths[i].year, yearMonths[i].month)[1] for i in range(len(yearMonths))]
        for i in range(len(colmerges)):
            if i == 0:
                mstart = self.startCol
                mend = mstart + colmerges[i] - 1
                self.targetWs.range((1, mstart), (1, mend)).merge()
            else:
                mstart = mend +1
                mend = mstart + colmerges[i] -1
                self.targetWs.range((1, mstart), (1, mend)).merge()
        # (4) Formatting scheduel template including Gantt chart ranges
        last_column = self.targetWs.range(3,1).end('right').lat_cell.column
        self.targetWs.range((self.startRow, self.startCol), (self.startRow + len(self.df), (dayEnd - dayOne).days + self.startCol)).column_width = self.colWidth
        self.targetWs.range((3, self.startCol), (3, last_column)).api.Orientation = 90
        self.targetWs.range((1, self.startCol), (self.startRow + len(self.df)-1, last_column)).api.Borders.Weight = 2 # thin
        self.targetWs.range((1, self.startCol), (self.startRow + len(self.df)-1, last_column)).api.Borders.Color = self.borderColor

class GanttChart:
    def __init__(self, ws, df, ps, startCol, startRow) -> None:
        self.targetWs = ws
        self.df = df
        self.size = len(self.df)
        self.start = min([self.df['Start'].loc[self.df['Predecessor'] == '']]).values[0]
        self.start = pd.to_datetime(self.start).date()
        self.dayone = date(self.start.year, self.start.month, 1)
        self.ps = ps
        self.startCol = int(startCol)
        self.startRow = int(startRow)
        self.last_column = self.targetWs.range(3,1).end('right').last_cell.column
        self.colWidth = 1.0 # unit in point
        self.rowHeight = 20 # unit in point and default is 15 points
        self.criticalColor = rgb_to_int((255,51,204))
        self.noncriticalColor = rgb_to_int((80,58,244))
        self.connectorColor = rgb_to_int((0,0,0))       
        self.lateColor = rgb_to_int((175,175,175))

    def earlyGantt(self):
        #(1) early chedule barts
        startDates = [self.df['Start'][i].date() for i in range(len(self.df['Start']))]
        startCols = [(s-self.dayone).days + self.startCol for s in startDates]
        barlength = [((self.df['Finish'][i] - self.df['Start'][i]).days +1) * self.colwidth_point - 0.2 * self.colwidth_point for i in range(self.size)] # number of columns per period
        bars = [None] * len(self.df) # Store each shapes instance for drawing connectors
        for i in range (len(self.df)):
            if startCols[i] <= self.last_column:
                left = self.targetWs.range(self.startRow + i, startCols[i]).left + 0.1 * self.colWidth
                top = self.targetWs.range(self.startRow + i, startCols[i]).top + 0.1 * self.rowHeight
                width,height = barlength[i],self.rowHeight*0.35

                bars[i] = self.targetWs.api.Shapes.AddShape(1, left, top, width, height)
                if self.df['Float'][i] == '0 days':
                    bars[i].Fill.ForeColor.RGB = self.criticalColor
                else:
                    bars[i].Fill.ForeColor.RGB = self.noncriticalColor
            # (2) produce connectors to show schedule activitties relationship based on early schedule bars
            for i in range(len(self.ps)):
                ipred = self.ps['predid'][i]
                isucc = self.ps['succid'][i]
                if ipred != '':
                    if startCols[int(ipred)] < self.last_column and startCols[int(isucc)] < self.last_column:
                        ibegin = 4
                        iend = 2
                        connector = self.targetWs.api.Shapes.AddConnector(2, left, top, 1,1) # connector type 2 for elbow connector               
                        connector.Line.EndArrowheadStyle = 4 # end arrow head style 4 for stealth bomber                 
                        connector.Line.ForeColor.RGB = self.connectorColor                   
                        connector.ConnectorFormat.BeginConnect(bars[int(ipred)], ibegin)                   
                        connector.ConnectorFormat.EndConnect(bars[int(isucc)], iend)       

    def lateGantt(self):
        #(1) Late schedule bars without need to add conectors
        startDates = [self.df['Late Start'][i].date() for i in range(len(self.df['Late Start']))]
        startCols = [(s-self.dayone).days + self.startCol for s in startDates]
        barlength = [((self.df['Late Finish'][i] - self.df['Late Start'][i]).days +1) * self.colwidth_point - 0.2 * self.colwidth_point for i in range(self.size)] # number of columns per period
        
        for i in range (len(self.df)):
            if startCols[i] <= self.last_column:
                left = self.targetWs.range(self.startRow + i, startCols[i]).left + 0.1 * self.colWidth
                top = self.targetWs.range(self.startRow + i, startCols[i]).top + 0.1 * self.rowHeight
                width,height = barlength[i],self.rowHeight*0.35
                bar_late = self.targetWs.api.Shapes.AddShape(1, left, top, width, height)
                bar_late.Fill.ForeColor.RGB = self.lateColor
                bar_late.ZOrder(1) #ZOrdercmd is parametrer, 1 for Send to Back


if __name__ == '__main__':
    sch = BasicCPA()
    sch.startCPM()
    periods = sch.projectPeriod()
    sch.CPA(periods)
