from sqlalchemy import create_engine, inspect
import pandas as pd
import numpy as np

import psycopg2 

# Conenct to Database
connection_str = 'postgresql+psycopg2://iotadmin:controlsamin@10.1.175.103/BensonHill_MES_example'
engine = create_engine(connection_str)
# try:
#     with engine.connect() as connection_str:
#         print('Successfully connected to the PostgreSQL database')
# except Exception as ex:
#     print(f'Sorry failed to connect: {ex}')

# Create an inspector object
inspector = inspect(engine)

# Get the table names
table_names = inspector.get_table_names()

# Print the table names
for table_name in table_names:
    print(table_name)


def main():
    projects = pd.read_sql_table('projects', con = engine, schema="CSH_Gantt")
    workflows = pd.read_sql_table('workflows', con = engine, schema="CSH_Gantt")
    workflowtasks = pd.read_sql_table('workflowtasks', con = engine, schema="CSH_Gantt")
    tasksdefined = pd.read_sql_table('tasksdefined', con = engine, schema="CSH_Gantt")
    milestones = pd.read_sql_table('milestones', con = engine, schema="CSH_Gantt")
    #new_dependince = {'DependencyID':'', 'EventID':'', 'EventType': '' , 'PredecessorID':'','PredecessorType':'','SuccessorID':'','SuccessorType': ''}
    tasks_df = pd.DataFrame(columns=['TaskID', 'ProjectID','WorkflowID','TaskName','StartDate','EndDate','Duration'])
    dependencies_df = pd.DataFrame(columns=['DependencyID', 'EventID', 'EventType','PredecessorID', 'PredecessorType', 'SuccessorID' ,'SuccessorType'])

    TasksFromProjectWorkflows = CreateTasksFromProjectWorkflows(tasks_df, projects,workflows,workflowtasks,tasksdefined)

    tasks,dependencies = Create_Tasks_and_Dependinces(projects,workflows,workflowtasks,tasksdefined)
    conn = engine.connect()


    tasks.to_sql('tasks', con=conn, if_exists='replace', schema="CSH_Gantt", index=False)
    dependencies['DependencyID'] = dependencies.index
    dependencies.to_sql('dependencies', con=conn, if_exists='replace', schema="CSH_Gantt", index=False)

def CreateTasksFromProjectWorkflows(task_df, projects,workflows,workflowtasks,tasksdefined):
    for i in range(len(projects)):
        project = projects.loc[[i]]
        projectID = project['ProjectID'].values[0]
        projectWorkflowID = project['WorkflowID'].values[0]
        projectWorkFlowTasks = (workflowtasks.loc[workflowtasks['WorkflowID'] == projectWorkflowID]).reset_index(drop=True)
        
        #go through workflow tasks
        #Create new tasks
        workflow_tasks = pd.DataFrame(columns=['TaskID', 'ProjectID','WorkflowID','TaskName','StartDate','EndDate','Duration','Workflow_pred','Workflow_succ','predtype','WorkflowTaskID'])
        for j in range(len(projectWorkFlowTasks)):
            newtask = {'ProjectID': '','WorkflowID': '','TaskName': '','StartDate': '','EndDate' :'','Duration':'','Workflow_pred':'','Workflow_succ':'','predtype':'','WorkflowTaskID':''}
            projectWorkFlowTask = projectWorkFlowTasks.loc[[j]]
            newtask['WorkflowTaskID'] = projectWorkFlowTask['WorkflowTaskID'].values[0]
            newtask['Workflow_pred'] = projectWorkFlowTask['WorkFloPreID'].values[0]
            newtask['Workflow_succ'] = projectWorkFlowTask['WorkFlowSuccID'].values[0]
            pretype = projectWorkFlowTask['predtype'].values[0]
            newtask['predtype'] = pretype
            #Get Task Name
            taskdefID = projectWorkFlowTask['TaskDefID'].values[0]
            taskdef = tasksdefined.loc[tasksdefined['TaskDefID'] == taskdefID]
            newtask['TaskName'] = taskdef['TaskName'].values[0]
            #Assign Project ID
            newtask['ProjectID'] = projectID
            #Assign Workflow ID
            newtask['WorkflowID'] = projectWorkflowID
            #Calculate Duration and assign
            units = taskdef['Units'].values[0]
            numOfUnits = project[units].values[0]
            cycletime = taskdef['Cycletime'].values[0]
            duration = numOfUnits * cycletime
            newtask['Duration'] = duration
            df_newtask = pd.DataFrame([newtask])
            # Check if either DataFrame is empty
            if workflow_tasks.empty:
                workflow_tasks = df_newtask
            else:
                # Concatenate only if both DataFrames have valid data
                workflow_tasks = pd.concat([workflow_tasks, df_newtask])

        #assign TaskID to workflowTask
        if tasks.empty:
            task_lastIndex = 0
            TaskID = 0
        else:
            task_lastIndex = len(tasks)
            TaskID = task_lastIndex
        
        workflow_tasks.reset_index(inplace=True, drop=True)
        for i in range(len(workflow_tasks)):
            workflow_tasks.loc[i, 'TaskID'] = int(TaskID)
            TaskID += 1
        
        #Add workflow Task to Tasks
        if tasks.empty:
            tasks = workflow_tasks
        else:
            # Concatenate only if both DataFrames have valid data
            tasks = pd.concat([tasks, workflow_tasks])   



def Create_Tasks_and_Dependinces(projects,workflows,workflowtasks,tasksdefined):
    tasks = pd.DataFrame(columns=['TaskID', 'ProjectID','WorkflowID','TaskName','StartDate','EndDate','Duration'])
    dependencies = pd.DataFrame(columns=['DependencyID', 'EventID', 'EventType','PredecessorID', 'PredecessorType', 'SuccessorID' ,'SuccessorType'])
    EventType = 1
    PredecessorType = 1
    SuccessorType = 1


    



    for i in range(len(projects)):
        project = projects.loc[[i]]
        projectID = project['ProjectID'].values[0]
        projectWorkflowID = project['WorkflowID'].values[0]
        projectWorkFlowTasks = (workflowtasks.loc[workflowtasks['WorkflowID'] == projectWorkflowID]).reset_index(drop=True)
        
        #go through workflow tasks
        #Create new tasks
        workflow_tasks = pd.DataFrame(columns=['TaskID', 'ProjectID','WorkflowID','TaskName','StartDate','EndDate','Duration','Workflow_pred','Workflow_succ','predtype','WorkflowTaskID'])
        for j in range(len(projectWorkFlowTasks)):
            newtask = {'ProjectID': '','WorkflowID': '','TaskName': '','StartDate': '','EndDate' :'','Duration':'','Workflow_pred':'','Workflow_succ':'','predtype':'','WorkflowTaskID':''}
            projectWorkFlowTask = projectWorkFlowTasks.loc[[j]]
            newtask['WorkflowTaskID'] = projectWorkFlowTask['WorkflowTaskID'].values[0]
            newtask['Workflow_pred'] = projectWorkFlowTask['WorkFloPreID'].values[0]
            newtask['Workflow_succ'] = projectWorkFlowTask['WorkFlowSuccID'].values[0]
            pretype = projectWorkFlowTask['predtype'].values[0]
            newtask['predtype'] = pretype
            #Get Task Name
            taskdefID = projectWorkFlowTask['TaskDefID'].values[0]
            taskdef = tasksdefined.loc[tasksdefined['TaskDefID'] == taskdefID]
            newtask['TaskName'] = taskdef['TaskName'].values[0]
            #Assign Project ID
            newtask['ProjectID'] = projectID
            #Assign Workflow ID
            newtask['WorkflowID'] = projectWorkflowID
            #Calculate Duration and assign
            units = taskdef['Units'].values[0]
            numOfUnits = project[units].values[0]
            cycletime = taskdef['Cycletime'].values[0]
            duration = numOfUnits * cycletime
            newtask['Duration'] = duration
            df_newtask = pd.DataFrame([newtask])
            # Check if either DataFrame is empty
            if workflow_tasks.empty:
                workflow_tasks = df_newtask
            else:
                # Concatenate only if both DataFrames have valid data
                workflow_tasks = pd.concat([workflow_tasks, df_newtask])

        #assign TaskID to workflowTask
        if tasks.empty:
            task_lastIndex = 0
            TaskID = 0
        else:
            task_lastIndex = len(tasks)
            TaskID = task_lastIndex
        
        workflow_tasks.reset_index(inplace=True, drop=True)
        for i in range(len(workflow_tasks)):
            workflow_tasks.loc[i, 'TaskID'] = int(TaskID)
            TaskID += 1
        
        #Add workflow Task to Tasks
        if tasks.empty:
            tasks = workflow_tasks
        else:
            # Concatenate only if both DataFrames have valid data
            tasks = pd.concat([tasks, workflow_tasks])

        #dependencies = pd.DataFrame(columns=['DependencyID', 'ProjectID','PredecessorTaskID','SuccessorTaskID','Type'])
        # Create Dependencies
        for i in range(len(workflow_tasks)):
            #(columns=['DependencyID', 'EventID', 'EventType','PredecessorID', 'PredecessorType', 'SuccessorID' ,'SuccessorType'])
            new_dependince = {'DependencyID':'', 'EventID':'', 'EventType': '' , 'PredecessorID':'','PredecessorType':'','SuccessorID':'','SuccessorType': ''}
            workflow_task = workflow_tasks.iloc[i]
            Workflow_pred = workflow_task['Workflow_pred']
            Workflow_succ = workflow_task['Workflow_succ']
            EventID = workflow_task['TaskID']
            if np.isnan(Workflow_pred):
                PredecessorTaskID = None
            else:
                PredecessorTaskID = workflow_tasks.loc[workflow_tasks['WorkflowTaskID'] == Workflow_pred, 'TaskID'].values[0]
            if np.isnan(Workflow_succ):
                SuccessorTaskID == None
            else:
                SuccessorTaskID = workflow_tasks.loc[workflow_tasks['WorkflowTaskID'] == Workflow_succ, 'TaskID'].values[0]
            new_dependince = {'DependencyID': '','EventID':EventID, 'EventType': EventType,'PredecessorID':PredecessorTaskID,'PredecessorType': EventType,'SuccessorID':SuccessorTaskID,'SuccessorType': EventType}
            df_new_dependince = pd.DataFrame([new_dependince])
            if dependencies.empty:
                dependencies = df_new_dependince
            else:
                dependencies = pd.concat([dependencies, df_new_dependince], ignore_index=True)
    

    # Clean up Frames
    tasks = tasks.drop(['Workflow_pred',  'Workflow_succ', 'predtype' , 'WorkflowTaskID'], axis=1)



    return tasks, dependencies



if __name__ == '__main__':
    main()








    










 





    




