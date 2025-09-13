import os
os.environ['KIVY_LOG_MODE'] = 'PYTHON'

from core.orchestrators import Orchestrators, StartSequence
from data.sqlalchemy import DBManager

from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.core.window import Window
from kivy.uix.label import Label
from kivy.uix.gridlayout import GridLayout
from kivy.uix.button import Button
from kivy.uix.scrollview import ScrollView
from kivy.lang import Builder
from kivy.uix.screenmanager import ScreenManager, Screen

from interface.new_period_popup import AddPeriodPopup

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
            ("Hours worked today", f"{stats.get('last_db_hrs', 0):.2f}"),
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
        Window.size = (500, 450)
        Window.minimum_width, Window.minimum_height = 500, 400
        Window.clearcolor = (0.1, 0.1, 0.1, 1)
                
        self.add_widget(self.display_options())

    def on_kv_post(self, base_widget):
        self.ids.stats_panel.refresh()

    def sync_and_refresh(self):
        Orchestrators.check_sp_sync()
        
        self.ids.stats_panel.refresh()

    def display_options(self):
        options = {
            "Display Period Hours (bars)": Orchestrators.plot_daily_hours_bars,
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

    def open_add_period(self):
        def on_submit(data: dict):
            log.info(data)

        AddPeriodPopup(on_submit=on_submit).open()    

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
        Button:
            id: add_period_btn
            text: "Add new period"
            font_size: 12
            size_hint: 1, 1
            height: 44
            on_press: root.open_add_period()

        Widget:
        Widget:

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

    # Not yet added:
    # # Scroll area with options (we fill options_gl in Python)
    # ScrollView:
    #     id: options_scroll
    #     size_hint: 1, 1
    #     GridLayout:
    #         id: options_gl
    #         cols: 1
    #         spacing: 10
    #         padding: 0, 10
    #         size_hint_y: None
    #         height: self.minimum_height
'''

Builder.load_string(KV_main_layout)

class MainWindows(App):
    def __init__(self):
        super(MainWindows, self).__init__()

    def build(self):
        exists = StartSequence.check_local_data_exists()
        
        if not exists:
            log.info("Generating from start")
            StartSequence.generate_from_start()

        return MainMenuLayout()