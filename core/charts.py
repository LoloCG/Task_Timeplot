import matplotlib.pyplot as plt
import numpy as np

from utils.logger import LoggerSingleton
log = LoggerSingleton().get_logger()

class Charts:
    @classmethod
    def plot_daily_stack_bar(cls, df):
        '''
        Stacked bar chart for each day, each segment representing each subject.
        Rather than displaying all data from the start of the period, it should 
        display 1 or at max 2 weeks at a time.  
        '''
        
        df = df.sort_values(by='date')

        df_pivot = df.pivot_table(
            index='date',
            columns='subject',
            values='time_spent_hrs',
            fill_value=0
        )
        plt.style.use('seaborn-v0_8-paper')
        
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.set_axisbelow(True)
        ax.grid(True, which='major',axis='y',ls='-')

        dates = df_pivot.index  
        bottoms = np.zeros(len(df_pivot)) # generate array of 0 the length of days
        for subject in df_pivot.columns:
            values = df_pivot[subject].to_numpy()
            ax.bar(dates, values, bottom=bottoms, label=subject) # **bar_param
            bottoms += values

        ax.legend(loc='upper right', frameon=True) # , labelcolor='0.8'

        ax.set_xlabel('Date') # , color='0.8'
        plt.xticks(rotation=45)

        ax.set_ylim(bottom=0)
        ax.set_ylabel('Time Spent (Hours)')  #, color='0.8'
 
        plt.tight_layout()
        plt.show()
        return

    @staticmethod
    def plot_daily_subj_hours_line(df, current_course=None, add_avg=False, roll_avg=None):
        import matplotlib as mpl
        import matplotlib.pyplot as plt
        import matplotlib.patheffects as path_effects # prev PathEffects
        from matplotlib.ticker import MaxNLocator
        import matplotlib.colors as cm
        '''
            Plots a line chart showing the time spent on different subjects over a period of time.
        '''
        def avg_past_courses(df, current_course):
            df_past = df[(df['course'] != current_course)].copy()

            df_avg = df_past.groupby('date', as_index=False)['time_spent_hrs'].mean()

            df_avg['course'] = 'Average'
            df_avg['period'] = 'Average'
            
            return df_avg

        def roll_avgs(df, period_list):
            df_avg_list = []
            for unique_period in period_list:
                course, period = unique_period.split(';')
                period_data = df[(df['course'] == course) & (df['period'] == period)].sort_values('date')
                period_data['rolled_time_spent_hrs'] = period_data['time_spent_hrs'].rolling(window=roll_avg, min_periods=1).mean()
                df_avg_list.append(period_data)

            df_rolled = pd.concat(df_avg_list, ignore_index=True)

            return df_rolled

        df = df.sort_values(by='date')
        df = df[['course','period', 'date', 'time_spent_hrs']]    
        df = df.groupby(['course', 'period', 'date'], as_index=False)['time_spent_hrs'].sum().reset_index()

        if add_avg:
            df_avg_past = avg_past_courses(df, current_course)
            df = pd.concat([df, df_avg_past[['course', 'period', 'date', 'time_spent_hrs']]], axis=0, ignore_index=True)

        period_list = [] 
        for course in df['course'].unique():
            course_data = df[(df['course'] == course)]
            for period in course_data['period'].unique():
                unique_period = str(course + ';' + period)
                period_list.append(unique_period)

        if roll_avg:
            df = roll_avgs(df, period_list)
            
        plt.style.use('bmh')

        fig, ax = plt.subplots(figsize=(11, 6))

        # cmap = cm.get_cmap('Set1', len(period_list)) # Dark2, Set1, inferno, prism
        cmap = mpl.colormaps['Set1'].resampled(len(period_list))
        # color_cycle = cycler(color=['#4F81BD', '#C0504D', '#9BBB59', '#8064A2'])  
        

        fig.set_facecolor('#444444') 
        current_line_params = {
            'alpha':        0.8, 
            'ls':           '-', 
            'linewidth':    1.7,
            'color':        '0.9',
            'zorder':       1,
        }
        line_params = {
            'alpha':        0.7, 
            'ls':           ':', 
            'linewidth':    1.5,
            'zorder':       1
        }  
        avg_line_params = {
            'alpha':        0.8, 
            'ls':           '-', 
            'linewidth':    2.25,
            'color':        '0.7',
            'zorder':       2,
        }
        path_efx_avg = [path_effects.SimpleLineShadow(offset=(0.5, -1), shadow_color='white'), path_effects.Normal()]
        
        if roll_avg is not None:
            plot_data = 'rolled_time_spent_hrs'
        else: plot_data = 'time_spent_hrs'

        n = 0
        for unique_period in period_list:
            course, period = unique_period.split(';')
            period_data = df[(df['course'] == course) & (df['period'] == period)].sort_values('date')

            if period == 'Average': 
                ax.plot(period_data['date'], period_data[plot_data], 
                    label=f'{period}', **avg_line_params,
                    path_effects=path_efx_avg)
                continue
            
            if course == current_course: # this will have to be modified for more than 1 semester... this is a quick fix...
                ax.plot(period_data['date'], period_data[plot_data], 
                    label=f'{course} - {period}',
                    **current_line_params,
                    path_effects=path_efx_avg)
                continue

            ax.plot(period_data['date'], period_data[plot_data], label=f'{course} - {period}', color=cmap(n), **line_params)
            n += 1

        # ax.set_xlim(left=0)
        ax.set_xlim(left=df['date'].min())
        ax.set_ylim(bottom=0)
        ax.xaxis.set_major_locator(MaxNLocator(nbins=10)) 
        ax.tick_params(colors='0.8')
        ax.set_xlabel('date', color='0.8')  # Label for the X axis (date)
        ax.set_ylabel('Time Spent (Hours)', color='0.8')  

        ax.set_facecolor('#444444')
        
        plt.xticks(rotation=45)

        ax.legend(loc='upper left', labelcolor='0.8', frameon=False) # , framealpha=0.2
        plt.tight_layout()
        plt.show()
