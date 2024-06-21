import pandas as pd
import numpy as np
import math

from datetime import datetime, timedelta
#from GanttDB import GanttDB

#TODO place this into GantDB file and import into main
class GanttDB:
    def __init__(self, filepath):
        self.ScheduleEventsHasChanged = True
        self.dfs= self.read_excel_into_dfs(filepath)
        self.DependencyType = self.dfs['DependencyType']
        self.ScheduleEventTypes = self.dfs['ScheduleEventTypes']
        self.Milestones = self.dfs['Milestones']
        self.Sprintes = self.dfs['Sprints']
        self.Projects = self.dfs['Projects']
        self.Workflows = self.dfs['Workflows']
        self.WorkflowTasks = self.dfs['WorkflowTasks']
        self.TasksDefined = self.dfs['TasksDefined']
        self.Tasks = self.dfs['Tasks']
        self.ScheduleEvents = pd.DataFrame(columns=['UID','EventType','EventID','PredecessorUID','PredecessorType','PredDepType','SuccessorUID','SuccessorType','SuccDepType', 'StartDate',	'EndDate'])
        self.ScheduleEvents_framework_columns = ['UID','EventType','EventID','PredecessorUID','PredecessorType','PredDepType','SuccessorUID','SuccessorType','SuccDepType', 'StartDate',	'EndDate']
        self.tasks_columns = ['TaskID',	'ProjectID',	'WorkflowID',	'TaskName',	'StartDate',	'EndDate',	'Duration', 'WorkflowTaskID']
        self.ScheduleEvents_framework = pd.DataFrame(columns=self.ScheduleEvents_framework_columns)
        

        self.create_schedule_events()
        self.process_schedule_dependencys()
        

        while True:
            self.ScheduleEventsHasChanged = False
            self.checkForStartDates()
            self.checkProjects()
            self.checkTasksforEndDate()
            self.UpdateSuccessors()

            if self.ScheduleEventsHasChanged == False:
                break 

        self.ScheduleEvents.to_csv('ScheduleEvents.csv',index=False)
        self.Tasks.to_csv('Tasks.csv',index=False)
        

        print(self.ScheduleEvents)
    
   
    def checkTasksforEndDate(self):
        scheduleTasks = self.ScheduleEvents.loc[(self.ScheduleEvents['EventType']=='task') & (self.ScheduleEvents['StartDate']!= '') & (self.ScheduleEvents['EndDate'] == '')]
        for i in range(len(scheduleTasks)):
            scheduleTask = scheduleTasks.iloc[i]
            UID = scheduleTask['UID']
            enddate = self.getTaskEndDate(UID)
            self.ScheduleEvents.at[UID, 'EndDate'] = enddate
            self.ScheduleEventsHasChanged = True

            
            

        pass

    def read_excel_into_dfs(self,file_path):
        # Read all sheets into a dictionary of DataFrames
        dfs = pd.read_excel(file_path, sheet_name=None)
        return dfs

    def process_milestones(self):
        EventType = 'milestone'
        milestones = self.Milestones
        df = pd.DataFrame(columns=self.ScheduleEvents_framework_columns)
        
        for i in range(len(milestones)):
            milestone = milestones.iloc[i]
            EventID = milestone.MilestoneID
            new_row = ['',EventType, EventID ,'','','','','','','','']
            df.loc[len(df)] = new_row
        
        self.ScheduleEvents = pd.concat([self.ScheduleEvents,df])

    def process_projects(self):
        EventType = 'project'
        projects = self.Projects
        df = pd.DataFrame(columns=self.ScheduleEvents_framework_columns)
        for i in range(len(projects)):
            project = projects.iloc[i]
            EventID = project.ProjectID
            new_row = ['',EventType, EventID ,'','','','','','','','']
            df.loc[len(df)] = new_row      
        self.ScheduleEvents = pd.concat([self.ScheduleEvents,df])   
        self.process_projectworkflows()

    def process_projectworkflows(self):
        projects = self.Projects
        df = pd.DataFrame(columns=self.tasks_columns)

        for i in range(len(projects)):
            project = projects.loc[[i]]
            projectID = project['ProjectID'].values[0]
            WorkflowID = project['WorkflowID'].values[0]
            WorkFlowTasks = (self.WorkflowTasks.loc[self.WorkflowTasks['WorkflowID'] == WorkflowID]).reset_index(drop=True)

            #workflow_tasks = pd.DataFrame(columns=['TaskID', 'ProjectID','WorkflowID','TaskName','StartDate','EndDate','Duration','Workflow_pred','Workflow_succ','predtype','WorkflowTaskID'])
            for j in range(len(WorkFlowTasks)):
                #newtask = {'ProjectID': '','WorkflowID': '','TaskName': '','StartDate': '','EndDate' :'','Duration':'','Workflow_pred':'','Workflow_succ':'','predtype':'','WorkflowTaskID':''}
                projectWorkFlowTask = WorkFlowTasks.loc[[j]]
                WorkflowTaskID = projectWorkFlowTask['WorkflowTaskID'].values[0]
                #Get Task Name
                taskdefID = projectWorkFlowTask['TaskDefID'].values[0]
                taskdef = self.TasksDefined.loc[self.TasksDefined['TaskDefID'] == taskdefID]
                TaskName = taskdef['TaskName'].values[0]

                #Calculate Duration and assign
                units = taskdef['Units'].values[0]
                numOfUnits = project[units].values[0]
                cycletime = taskdef['Cycletime'].values[0]
                duration = numOfUnits * cycletime
                new_row = ['',	projectID,	WorkflowID,	TaskName,	'',	'',	duration, WorkflowTaskID]
                df.loc[len(df)] = new_row
        
        self.Tasks = pd.concat([self.Tasks,df])  

    def process_tasks(self):
        self.Tasks['TaskID'] = self.Tasks.index
        EventType = 'task'
        tasks = self.Tasks
        df = pd.DataFrame(columns=self.ScheduleEvents_framework_columns)
        
        for i in range(len(tasks)):
            task = tasks.iloc[i]
            EventID = task.TaskID
            new_row = ['',EventType, EventID ,'','','','','','','','']
            df.loc[len(df)] = new_row
        
        self.ScheduleEvents = pd.concat([self.ScheduleEvents,df]) 
        pass
            
    def create_schedule_events(self):
        self.process_milestones()
        self.process_projects()
        self.process_tasks()
        self.ScheduleEvents = self.ScheduleEvents.reset_index(drop=True)
        self.ScheduleEvents['UID'] = self.ScheduleEvents.index

    def process_schedule_dependencys(self):
        for i in range(len(self.ScheduleEvents)):
            scheduleEvent = self.ScheduleEvents.loc[i]
            if scheduleEvent['EventType'] == 'task':
                eventtype = 'task'
                taskEvent = self.Tasks.loc[self.Tasks['TaskID'] == scheduleEvent['EventID']]
                projectID = taskEvent['ProjectID'].values[0]
                workflowID = taskEvent['WorkflowID'].values[0]
                WorkflowTaskID = taskEvent['WorkflowTaskID'].values[0]
                #Process Workflow
                if WorkflowTaskID != '':
                    workflowtask = self.WorkflowTasks.loc[self.WorkflowTasks['WorkflowTaskID'] == WorkflowTaskID]
                    WorkFloPreID = workflowtask.iloc[0]['WorkFloPreID']
                    WorkFlowSuccID = workflowtask.iloc[0]['WorkFlowSuccID']
                    WorkflowPredDepType = workflowtask['PredDepType'].values[0]
                    if np.isnan(WorkFloPreID):
                        UID = self.ScheduleEvents.loc[(self.ScheduleEvents['EventType'] == 'project')&(self.ScheduleEvents['EventID'] == projectID)]
                        UID = UID['UID'].values[0]
                        self.ScheduleEvents.at[i, 'PredecessorUID'] = UID
                        self.ScheduleEvents.at[i, 'PredecessorType'] = 'project'
                    else:
                        WorkFlowPreTask = self.Tasks.loc[(self.Tasks['ProjectID'] == projectID) & (self.Tasks['WorkflowID'] == workflowID) & (self.Tasks['WorkflowTaskID'] == WorkFloPreID)]
                        PreScheEvent = self.ScheduleEvents.loc[(self.ScheduleEvents['EventType'] == eventtype) & (self.ScheduleEvents['EventID'] == WorkFlowPreTask['TaskID'].values[0])] 
                        self.ScheduleEvents.at[i, 'PredecessorUID'] = PreScheEvent['UID'].values[0]
                        self.ScheduleEvents.at[i, 'PredecessorType'] = eventtype
                    if np.isnan(WorkFlowSuccID):
                        UID = self.ScheduleEvents.loc[(self.ScheduleEvents['EventType'] == 'project')&(self.ScheduleEvents['EventID'] == projectID)]
                        UID = UID['UID'].values[0]
                        self.ScheduleEvents.at[i, 'SuccessorUID'] = UID
                        self.ScheduleEvents.at[i, 'SuccessorType'] = 'project'
                    else:
                        WorkFlowSuccTask = self.Tasks.loc[(self.Tasks['ProjectID'] == projectID) & (self.Tasks['WorkflowID'] == workflowID) & (self.Tasks['WorkflowTaskID'] == WorkFlowSuccID)]
                        SuccScheEvent = self.ScheduleEvents.loc[(self.ScheduleEvents['EventType'] == eventtype) & (self.ScheduleEvents['EventID'] == WorkFlowSuccTask['TaskID'].values[0])] 
                        self.ScheduleEvents.at[i, 'SuccessorUID'] = SuccScheEvent['UID'].values[0]
                        self.ScheduleEvents.at[i, 'SuccessorType'] = eventtype
                    
                    if str(WorkflowPredDepType) == 'nan':
                        pass
                    else:
                        self.ScheduleEvents.at[i, 'PredDepType'] = WorkflowPredDepType
                        
            elif scheduleEvent['EventType'] == 'project':
                eventtype = 'project'
                projectEvent = self.Projects.loc[self.Projects['ProjectID'] == scheduleEvent['EventID']]
                projectID = projectEvent['ProjectID'].values[0]
                PreID = projectEvent['PredecessorID'].values[0]
                if np.isnan(PreID):
                    pass
                else:
                    PreType = projectEvent['PredecessorType'].values[0]
                    PredDepType =projectEvent['PredDepType'].values[0]
                    PreScheEvent = self.ScheduleEvents.loc[(self.ScheduleEvents['EventType'] == PreType) & (self.ScheduleEvents['EventID'] == PreID)]
                    self.ScheduleEvents.at[i, 'PredecessorUID'] = PreScheEvent['UID'].values[0]
                    self.ScheduleEvents.at[i, 'PredecessorType'] = PreType
                    self.ScheduleEvents.at[i, 'PredDepType'] = PredDepType

            elif scheduleEvent['EventType'] == 'milestone':
                            eventtype = 'milestone'
                            milestone = self.Milestones.loc[self.Milestones['MilestoneID'] == scheduleEvent['EventID']]
                            StartDate = milestone['Date'].values[0]
                            self.ScheduleEvents.at[i, 'StartDate'] = StartDate
                            self.ScheduleEvents.at[i, 'EndDate'] = StartDate
            else:
                print(f"issue with Eventy Type of ScheduleEventUID: {i}")

    def getTaskEndDate(self,UID):
        scheduletask = self.ScheduleEvents.iloc[UID]
        task = self.Tasks.iloc[scheduletask['EventID']]
        timediff = timedelta(seconds=task['Duration'])
        startTime = scheduletask['StartDate']
        if startTime == '':
            return ''
        if isinstance(startTime, datetime) == False:
            startTime = self.to_datetime(startTime)
        enddate = startTime + timediff
        return enddate


    def checkForStartDates(self):
        for i in range(len(self.ScheduleEvents)):
            scheduleEvent = self.ScheduleEvents.iloc[i]
            if scheduleEvent['StartDate'].values[0] == '' and scheduleEvent['PredecessorUID'] != '':
                predEvent = self.ScheduleEvents.loc[self.ScheduleEvents['UID'] == scheduleEvent['PredecessorUID']]
                if predEvent['EndDate'].values[0] != '':
                    self.ScheduleEvents.at[i, 'StartDate'] = predEvent['EndDate'].values[0]
                    self.ScheduleEventsHasChanged = True
                elif (predEvent['EventType'].values[0] == 'project') & (scheduleEvent['EventType'] == 'task') & (predEvent['StartDate'].values[0] != ''):
                    self.ScheduleEvents.at[i, 'StartDate'] = predEvent['StartDate'].values[0]
                    enddate = self.getTaskEndDate(i)
                    self.ScheduleEvents.at[i, 'EndDate'] = enddate
                    self.ScheduleEventsHasChanged = True

    def checkProjects(self):
            ProjectEventsToProcess = self.ScheduleEvents.loc[(self.ScheduleEvents['EventType']=='project')&(self.ScheduleEvents['StartDate'] != '')&(self.ScheduleEvents['EndDate'] == '')]
            for i in range(len(ProjectEventsToProcess)):
                ProjectEvent = ProjectEventsToProcess.iloc[i]
                Project = self.Projects.loc[self.Projects['ProjectID'] == ProjectEvent['EventID']]
                TasksInProject = self.Tasks.loc[self.Tasks['ProjectID'] == ProjectEvent['EventID']]
                workflowTasks = self.WorkflowTasks.loc[self.WorkflowTasks['WorkflowID'] == Project['WorkflowID'].values[0]]

    def UpdateSuccessors(self):
        EventsToProcess = self.ScheduleEvents.loc[(self.ScheduleEvents['EndDate'] != '')&(self.ScheduleEvents['SuccessorUID'] != '')]
        for i in range(len(EventsToProcess)):
            EventToProcess = EventsToProcess.iloc[i]
            successorEventUID = EventToProcess['SuccessorUID']
            successor = self.ScheduleEvents.iloc[successorEventUID]
            if successor['StartDate'] == '':
                self.ScheduleEvents.at[successorEventUID, 'StartDate'] = EventsToProcess['EndDate']
                self.ScheduleEventsHasChanged = True

        


            #     if predEvent['EventType'].values[0] == 'milestone':
            #         self.ScheduleEvents.at[i, 'StartDate'] = predEvent['StartDate'].values[0]
            #     if predEvent['EventType'].values[0] == 'task':
            #         pass
            # elif scheduleEvent['EventType'] == 'task':
            #     task = self.Tasks.loc[self.Tasks['TaskID'] == scheduleEvent['EventID']]
            #     if task['WorkflowID'].values[0] != '':
            #         workflow = (self.WorkflowTasks[self.WorkflowTasks['WorkflowID'] == task['WorkflowID'].values[0]])
            #         firstTaskInWorkflow = workflow.iloc[0]
            #         if firstTaskInWorkflow['WorkflowTaskID'] == task['WorkflowTaskID'].values[0]:
            #             project = self.ScheduleEvents.loc[(self.ScheduleEvents['EventType'] == 'project') & (self.ScheduleEvents['EventID'] == task['ProjectID'].values[0])]
            #             self.ScheduleEvents.at[i, 'StartDate'] = project['StartDate'].values[0]
            #             enddate = self.process_task_enddate(i)
            #             self.ScheduleEvents.at[i, 'EndDate'] = enddate

    def process_task_enddate(self,UID):
        scheduletask = self.ScheduleEvents.iloc[UID]
        task = self.Tasks.iloc[scheduletask['EventID']]
        timediff = timedelta(seconds=task['Duration'])
        
        startTime = scheduletask['StartDate']
        startTime = self.to_datetime(startTime)
        enddate = startTime + timediff
        return enddate
                
    def to_datetime(self, date):
        """
        Converts a numpy datetime64 object to a python datetime object 
        Input:
        date - a np.datetime64 object
        Output:
        DATE - a python datetime object
        """
        timestamp = ((date - np.datetime64('1970-01-01T00:00:00'))
                    / np.timedelta64(1, 's'))
        return datetime.utcfromtimestamp(timestamp)



                




                

            






def main():
    # Specify the path to your Excel file

    file_path = 'DB_V2.xlsx'
    DB = GanttDB(file_path)

if __name__ == '__main__':
    main()


