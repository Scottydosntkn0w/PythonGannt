import sys
import pandas as pd
import numpy as np
import xlwings as xw
from datetime import datetime, timedelta
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
        self.wb = xw.books.active
        targetWSName = 'Deterministic'
        try:
            self.targetWS = self.wb.sheets(targetWSName)
        except:
            sys.exit()
        self.targetCell = 'E3' # top left cell of outcome data
        self.floatCol = 9      # Coumn number for float
        self.startCol = int(11)# Gantt chart start column number
        self.startRow = int(4) # Gantt chart start row number
        self.templMonths = int(4) # Schedule template coverage months

    def __cleanUp(self):# Remove existing charts, data tables, and shapes
        self.wb.app.screen_updating = False
        if len(self.targetWS.charts) > 0:
            for _ in range(len(self.targetWS.charts)):
                self.targetWS.charts[0].delete()
        self.targetWS[:, self.startCol-1:37].delete()
        if len(self.targetWS.shapes):
            for sh in self.targetWS.shapes:
                sh.delete()
        self.wb.app.screen_updating = True

    def __readExcel(self):
        try:
            self.wss=self.wb.sheets['deterministic']
        except:
            sys.exit()
        self.df = pd.read_excel(io = self.wb.fullname, sheet_name=self.wss.name,index_col=None, usecols='A:I',skiprows=2,na_filter=False)
        self.size = len(self.df)
    
    def __validateData(self): # to be detailedi n practical scheduling
        pass
    def __projectCalendar(self):# to be detailed in practical scheduling
        pass

    def __schTemplate(self):
        template = scheduleTemplate(self.targetWS, self.df, self.startCol, self.startRow, self.templMonths)
        self.wb.app.screen_updating = False
        template.timeTable()
        self.wb.app.screen_updating = True
    
class scheduleTemplate:
    def __init__(Ws, df, startCol, startRow, templMonths) -> None:
        # Tbd
        pass
    
    def timeTable(self):
        # Tbd
        pass

    def startCPM(self):
        """When Gantt chart is displayed,
        the scheudle template is required.
        """
        self.__cleanUP()
        self.__readExcel()
        self.__validateData()
        self.__projectCalendar()
        self.__schTemplate()

    def startCPMwoGantt(self):

        self.__cleanUP()
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
