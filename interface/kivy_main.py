import os
from tkinter import Tk, filedialog

os.environ['KIVY_LOG_MODE'] = 'PYTHON'

from core.orchestrators import Orchestrators, StartSequence
from data.sqlalchemy import DBManager
from data.config_handler import ConfigManager

from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.core.window import Window
from kivy.uix.label import Label
from kivy.uix.gridlayout import GridLayout
from kivy.uix.button import Button
from kivy.uix.scrollview import ScrollView
from kivy.lang import Builder
from kivy.uix.screenmanager import ScreenManager, Screen

from interface.popups import (
    AddPeriodPopup,
    ExcludeSubjectsPopup,
    SyncFilePopup,
)

from utils.logger import LoggerSingleton
log = LoggerSingleton().get_logger()

class StatsPanel(GridLayout):
    def __init__(self,stats: dict | None = None, **kwargs): # 
        super().__init__(**kwargs)
                
        if stats is not None: self.populate(stats)

    def populate(self, stats: dict):
        self.clear_widgets()
        rows = [
            ("Last sync", stats.get("last_sync", "-")),
            ("Last day in DB", stats.get("last_db_day", "-")),
            ("Hours worked last day", f"{stats.get('last_db_hrs', 0):.2f}"),
            ("This week hours", f"{stats.get('total_week_hours', 0):.2f}"),
            ("Week daily avg", f"{stats.get('avg_week_daily', 0):.2f}"),

        ]
        for label, value in rows:
            self.add_widget(
                Label(
                    text=f"[b]{label}:[/b]", markup=True, halign="right", valign="middle"
                )
            )
            self.add_widget(Label(text=str(value), halign="left", valign="middle"))

    def refresh(self):
        """Pull latest stats and repopulate."""
        stats = Orchestrators.get_basic_stats()
        self.populate(stats)

class MainMenuLayout(BoxLayout):
    def __init__(self, **kwargs):
        super(MainMenuLayout, self).__init__(**kwargs)
        Window.size = (400, 550)
        Window.minimum_width, Window.minimum_height = 500, 400
        Window.clearcolor = (0.1, 0.1, 0.1, 1)
                
        self.add_widget(self.display_options())

    def sync_and_refresh(self):
        Orchestrators.check_sp_sync()
        
        self.ids.stats_panel.refresh()

    def display_options(self):
        options = {
            "Display Period Hours (bars)": Orchestrators.plot_daily_hours_bars,
            "Display Weekly Hours (bars)": Orchestrators.plot_weekly_hours_bars,
            "Display total Period Hours": Orchestrators.plot_total_horus_bars,
            "Display week average (Line)": Orchestrators.plot_week_avg_line_compared,
            "test": lambda *_: log.error("Feature not added"),
        }
        
        scroll = ScrollView(size_hint=(1, 1))
        glayout = GridLayout(cols=1, spacing=10, size_hint_y=None, padding=(0, 10))
        glayout.bind(minimum_height=glayout.setter("height"))
        for label, func in options.items():
            btn = Button(text=label, size_hint=(0.95, None), height=50)
            btn.bind(on_press=func)
            glayout.add_widget(btn)
        scroll.add_widget(glayout)
        return scroll

    def open_past_data_import(self, *_):
        dialog_root = Tk()
        dialog_root.withdraw()
        try:
            selected_path = filedialog.askopenfilename(
                parent=dialog_root,
                title="Select historical CSV file",
                initialdir=os.path.expanduser("~"),
                filetypes=(("CSV files", "*.csv"),),
            )
        finally:
            dialog_root.destroy()

        if selected_path:
            try:
                df = Orchestrators.import_past_data(
                    selected_path,
                    request_review=self.show_period_review,
                )
            except (OSError, ValueError) as exc:
                log.error(f"Could not read historical CSV: {exc}")
            else:
                log.info(f"Validated {len(df)} historical CSV rows (import not implemented): {selected_path}")

    def show_period_review(self, review_data: dict, submit_settings):
        AddPeriodPopup(
            on_submit=submit_settings,
            start_date=review_data["start_date"],
            course_name=review_data["course_name"],
            period_name=review_data["period_name"],
            subjects_list=review_data["subjects"],
            auto_exclude=review_data["auto_exclude"],
            show_exclusions=True,
        ).open()

    # def open_add_period(self):
    #     def on_submit(data: dict): 
    #         log.info(data)
    #         log.info(f"Feature not yet added.")
    #     AddPeriodPopup(on_submit=on_submit).open() 

    def open_exclude_subjects(self):
        def on_submit(data: dict): 
            log.info(data)
   
        ExcludeSubjectsPopup( 
            cnfg_mng=ConfigManager,
            db_mng=DBManager,
            on_submit=on_submit
        ).open() 

KV_main_layout='''
<StatsPanel>:
    cols: 2
    spacing: 20
    size_hint: 1, None
    height: self.minimum_height

<MainMenuLayout>:
    orientation: "vertical"
    spacing: 20
    padding: 10

    # Top row: "Add new period" button aligned left
    BoxLayout:
        size_hint_y: None
        height: 44
        spacing: 10
        # Button:
        #     id: add_period_btn
        #     text: "Add new period"
        #     font_size: 12
        #     size_hint: 1, 1
        #     height: 44
        #     on_press: root.open_add_period()

        Button:
            id: import_csv_btn
            text: "Import historical CSV..."
            font_size: 12
            size_hint: 1, 1
            height: 44
            text_size: self.width - 8, self.height
            halign: "center"
            valign: "middle"
            on_press: root.open_past_data_import()

        Button:
            id: exclude_subjects_btn
            text: "Exclude subjects"
            font_size:12
            size_hint: 1, 1
            height: 44
            on_press: root.open_exclude_subjects()
    
    # Title
    Label:
        text: "Study Hours Analytics"
        markup: True
        font_size: "24sp"
        size_hint_y: None
        height: 60
        halign: "center"
        valign: "middle"
        text_size: self.size  # make halign/valign effective

    StatsPanel:
        id: stats_panel

    Button:
        id: sync_btn
        text: "Sync with SP"
        size_hint_y: None
        height: 50
        on_press: root.sync_and_refresh()

'''

Builder.load_string(KV_main_layout)

class MainWindows(App):
    def __init__(self):
        super(MainWindows, self).__init__()

    def build(self):
        return MainMenuLayout()

    def on_start(self):
        start_dict = StartSequence.start_sequence()
        log.debug(f"on_start payload = {start_dict}")
        
        if start_dict["ask_current_period"] or start_dict["ask_sync_path"]:
            self.run_fresh_start_sequence(start_dict)
            return

        self.finish_startup()

    def run_fresh_start_sequence(self, start_payload):
        if start_payload["ask_sync_path"]:
            SyncFilePopup(on_submit=self.handle_sync_path).open()
            return

        if start_payload["ask_current_period"]:
            AddPeriodPopup(
                fresh_start=True,
                on_submit=self.handle_new_period,
            ).open()
            return
        
        self.finish_startup()

    def finish_startup(self):
        try:
            log.debug("Running startup sync check before first stats refresh")
            Orchestrators.check_sp_sync()
        except Exception as exc:
            log.warning(
                f"Startup sync failed, continuing with existing DB data: {exc}",
                exc_info=True,
            )

        if self.root and "stats_panel" in self.root.ids:
            self.root.ids.stats_panel.refresh()

        log.debug(f"returning to MainMenuLayout")
  
    def handle_sync_path(self, sync_path:str):
        ConfigManager().json_upsert({
            "sync_data": {
                "sync_file_path": sync_path,
            }
        })

        updated_payload = StartSequence.start_sequence()
        self.run_fresh_start_sequence(updated_payload)

    def handle_new_period(self, data:dict):
        log.debug(f"Generating data for the submitted current period")

        StartSequence.generate_from_start(
            ccourse=data["course_name"],
            cperiod=data["period_name"],
            period_start=data["start_date"],
        )

        if self.root and "stats_panel" in self.root.ids:
            self.root.ids.stats_panel.refresh()
