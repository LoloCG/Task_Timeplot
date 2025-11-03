import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from utils.logger import LoggerSingleton
log = LoggerSingleton().get_logger()

CHART_THEME = 'seaborn-v0_8-dark' #'seaborn-v0_8-darkgrid' # 'dark_background' # 'seaborn-v0_8-paper'
# Use the following after plt.style.use to create custom colors (?)  
# plt.rcParams.update({
#     "axes.facecolor": "#121212",
#     "figure.facecolor": "#121212",
#     "grid.color": "#444444",
#     "axes.labelcolor": "white",
#     "xtick.color": "white",
#     "ytick.color": "white",
#     "text.color": "white"
# })

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
        plt.style.use(CHART_THEME)
        
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.set_axisbelow(True)
        ax.grid(True, which='major',axis='y',ls='-')

        dates = df_pivot.index  
        bottoms = np.zeros(len(df_pivot)) # generate array of 0 the length of days
        for subject in df_pivot.columns:
            values = df_pivot[subject].to_numpy()
            ax.bar(
                dates, values, 
                bottom=bottoms, 
                label=subject,
                width=pd.Timedelta(days=0.925), 
                align='edge'
            ) # **bar_param
            bottoms += values

        ax.legend(loc='upper left', frameon=True) # , labelcolor='0.8'

        ax.set_xlabel('Date') # , color='0.8'
        plt.xticks(rotation=45)

        ax.set_ylim(bottom=0)
        ax.set_ylabel('Time Spent (Hours)')  #, color='0.8'
        
        start = df['date'].min().normalize()
        end = df['date'].max().normalize() + pd.Timedelta(days=1)
        ax.set_xlim(start, end)

        plt.tight_layout()
        plt.show()
        return

    @classmethod
    def plot_weekly_stack_bar(cls, weekly_df):
        weekly_df = weekly_df.sort_values(by='week_number').copy()

        df_pivot = weekly_df.pivot_table(
            index='week',
            columns='subject',
            values='time_spent_hrs',
            fill_value=0
        )
        df_pivot['total'] = df_pivot.sum(axis=1)

        log.debug(f"df_pivot weekly:\n{df_pivot}")

        return

        # TODO
        plt.style.use(CHART_THEME)
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.set_axisbelow(True)
        ax.grid(True, which='major',axis='y',ls='-')

        x = df_pivot.index
        ax.bar(x, values, bottom=bottoms, label=subject)

    @classmethod
    def plot_total_period_hours_bars(cls, df):
        if df is None or df.empty:
            log.error("No data to plot")
            return None

        dff = df.copy()
        dff = dff[dff["subject"].notna()]
        dff = dff[dff["subject"].astype(str).str.lower() != "none"]
        
        if dff.empty:
            log.warning("No valid subjects to plot after filtering")
            return None
        
        totals = (
            dff.groupby("subject", as_index=False)["time_spent_hrs"]
            .sum()
            .sort_values("time_spent_hrs", ascending=False)
        )
        log.debug("Grouped dataframe generated:")
        print(totals)

        title = f"{df['course'].iloc[0]} — {df['period'].iloc[0]}: total hours by subject"

        plt.style.use(CHART_THEME)
        fig, ax = plt.subplots(figsize=(10, 5))

        ax.bar(totals["subject"], totals["time_spent_hrs"])

        ax.set_axisbelow(True)
        ax.grid(True, which="major", axis="y", ls="-")

        ax.set_xlabel("Subject")
        ax.set_ylabel("Time Spent (Hours)")
        if title:
            ax.set_title(title)

        # Value labels on top of bars
        for i, v in enumerate(totals["time_spent_hrs"].to_numpy()):
            ax.text(i, v, f"{v:.2f}", ha="center", va="bottom", fontsize=12)

        plt.xticks(rotation=45, ha="right")
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
