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
        # log.debug(f"df_pivot daily stacked:\n{df_pivot}")
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
        """
        Stacked bar chart for each week, each segment representing each subject.
        X axis uses the week label (e.g. '2025-10-06/2025-10-12').
        """

        # Ensure weeks are sorted in time
        weekly_df = weekly_df.sort_values(by='week_number').copy()

        df_pivot = weekly_df.pivot_table(
            index='week',
            columns='subject',
            values='time_spent_hrs',
            fill_value=0
        )
        # total column kept for logging / checks, not plotted directly
        df_pivot['total'] = df_pivot.sum(axis=1)
        # log.debug(f"df_pivot weekly:\n{df_pivot}")

        plt.style.use(CHART_THEME)

        fig, ax = plt.subplots(figsize=(8, 4))
        ax.set_axisbelow(True)
        ax.grid(True, which='major', axis='y', ls='-')

        weeks = df_pivot.index
        x = np.arange(len(weeks))

        # start all stacks at zero
        bottoms = np.zeros(len(df_pivot))

        # subjects are all columns except 'total'
        subjects = [col for col in df_pivot.columns if col != 'total']

        for subject in subjects:
            values = df_pivot[subject].to_numpy()
            ax.bar(
                x,
                values,
                bottom=bottoms,
                label=subject,
            )
            bottoms += values

        ax.legend(loc='upper left', frameon=True)

        ax.set_xlabel('Week')
        ax.set_ylabel('Time Spent (Hours)')
        ax.set_ylim(bottom=0)

        ax.set_xticks(x)
        ax.set_xticklabels(weeks, rotation=45, ha='right')

        plt.tight_layout()
        plt.show()
        return
    
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

        print(totals.to_csv(sep=";", index=False, header=False))

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

    @classmethod
    def plot_rolling_7d_average(cls, df, window=7):
        """
        Line chart of rolling N-day average (default 7) of *daily total* study hours.
        One point per day (continuous dates), including 0 for days with no study.
        """
        # Work on a copy
        df = df.copy()

        # Ensure 'date' is datetime
        df['date'] = pd.to_datetime(df['date'])

        # 1) Aggregate to daily total hours (sum over subjects etc.)
        daily = (
            df.groupby('date', as_index=True)['time_spent_hrs']
              .sum()
              .sort_index()
              .rename('total_hours')
        )

        # 2) Build continuous daily index and fill missing days with 0
        full_index = pd.date_range(
            start=daily.index.min(),
            end=daily.index.max(),
            freq='D'
        )
        daily = daily.reindex(full_index, fill_value=0.0)
        daily.index.name = 'date'

        # 3) Rolling N-day mean (trailing window, including current day)
        rolling_avg = daily.rolling(window=window, min_periods=1).mean()

        # 4) Plot
        plt.style.use(CHART_THEME)
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.set_axisbelow(True)
        ax.grid(True, which='major', axis='y', ls='-')

        ax.plot(rolling_avg.index, rolling_avg.values, label=f'{window}-day rolling avg')

        ax.set_xlabel('Date')
        ax.set_ylabel(f'{window}-day rolling average (hours)')
        plt.xticks(rotation=45)

        ax.set_xlim(rolling_avg.index.min(), rolling_avg.index.max())
        ax.set_ylim(bottom=0)

        ax.legend(loc='upper left', frameon=True)

        plt.tight_layout()
        plt.show()
        return
  
    @classmethod
    def plot_rolling_7d_average_compared(cls, 
        df, window=7, 
        course_highlight:str|None =None, 
        period_highlight:str|None =None
    ):
        """
        Line chart of rolling N-day average (default 7) of *daily total* study hours.
        One point per day (continuous dates), including 0 for days with no study.
        """

        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        
        groups = []
        for (course, period), g in df.groupby(["course", "period"], sort=True):
            if g.empty:
                continue
            # log.debug(f"Computing series for {course} - {period}")
            
            daily = (
                g.groupby("date")["time_spent_hrs"]
                .sum()
                .sort_index()
            )

            # log.debug(f"earliest in series={g['date'].min()}")

            # force continuous days, filling with 0
            full_index = pd.date_range(
                start=daily.index.min(),
                end=daily.index.max(),
                freq="D"
            )
            daily = daily.reindex(full_index, fill_value=0.0)
            
            rolling = daily.rolling(window=window, min_periods=1).mean()

            day_idx = (rolling.index - rolling.index[0]).days

            groups.append({
                "course": course,
                "period": period,
                "x": day_idx,
                "y": rolling.values,
            })

    
        plt.style.use(CHART_THEME)
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.set_axisbelow(True)
        ax.grid(True, which='major', axis='y', ls='-')
        

        highlight_course = course_highlight if course_highlight is not None else None
        highlight_key = (
            (course_highlight, period_highlight)
            if course_highlight is not None and period_highlight is not None
            else None
        )

        for s in groups:
            key = (s["course"], s["period"])

            # If we are highlighting a course, skip plotting it here; we will plot it in the highlight section
            if highlight_course is not None and s["course"] == highlight_course:
                continue

            # If we are highlighting a specific (course, period), skip it here; plot later with emphasis
            if highlight_key is not None and key == highlight_key:
                continue

            ax.plot(
                s["x"],
                s["y"],
                alpha=0.35,
                linewidth=1.1,
                linestyle="--",  # dashed for non-highlight courses ("past courses")
                label=f"{s['course']} — {s['period']}",
            )


        if highlight_course is not None:
            # Plot ALL periods for the highlighted course (solid)
            for s in (g for g in groups if g["course"] == highlight_course):
                key = (s["course"], s["period"])
                
                log.debug(f"Highlighted df:\n{g}\n{s['y']}")
                
                # If a specific period is provided, make that one extra prominent
                is_primary = (highlight_key is not None and key == highlight_key)

                ax.plot(
                    s["x"],
                    s["y"],
                    alpha=0.95 if is_primary else 0.75,
                    linewidth=2.4 if is_primary else 1.9,
                    linestyle="-",
                    label=f"{s['course']} — {s['period']}",
                )


        ax.set_xlabel("Day (since start of period)")
        ax.set_ylabel(f"{window}-day rolling average (hours)")

        # X limits across all series (comparable timeline)
        x_min = min(int(min(s["x"])) for s in groups if len(s["x"]) > 0)
        x_max = max(int(max(s["x"])) for s in groups if len(s["x"]) > 0)
        ax.set_xlim(x_min, x_max)
        ax.set_ylim(bottom=0)

        ax.legend(
            loc="upper left",
            frameon=True,
            fontsize="x-small",
            handlelength=1.2,
            borderpad=0.25,
            labelspacing=0.25,
        )
        
        # if highlight_key is not None:
            # ax.legend(loc="upper left", frameon=True)

        plt.tight_layout()
        plt.show()