from datetime import datetime, timedelta
def dim_branch(**kwargs):
    time_now = datetime.now()
    time_diff = time_now - kwargs['execution_date']
    print('time diff: ', time_diff)
    print('start_date: ', kwargs['dag'].start_date)
    print('execution date: ', kwargs['execution_date'])
    
    # if dag run is the first interval or dag run is current then load dimension tables
    if kwargs['execution_date'] == kwargs['dag'].start_date or time_diff < timedelta(hours=1):
        return ['stage_users_dim',
               'stage_projects_dim',
               'stage_videos_dim']
    else:
        return 'skip_dimension_tables'