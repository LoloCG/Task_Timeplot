from kivy.uix.popup import Popup
from kivy.lang import Builder
from kivy.properties import StringProperty, ObjectProperty
from kivy.uix.togglebutton import ToggleButton

from utils.logger import LoggerSingleton
log = LoggerSingleton().get_logger()

AddPeriodPopupKv = '''
<AddPeriodPopup>:
    title: "Add Period"
    size_hint: 0.85, 0.6
    auto_dismiss: True

    BoxLayout:
        orientation: "vertical"
        padding: 16
        spacing: 12

        GridLayout:
            cols: 2
            row_default_height: "36dp"
            row_force_default: True
            size_hint_y: None
            height: self.minimum_height
            spacing: 8

            Label:
                text: "Start date"
                halign: "left"
                valign: "middle"
                text_size: self.size
            TextInput:
                text: root.start_date
                hint_text: "e.g., 20-06-2025"
                multiline: False
                write_tab: False
                on_text: root.start_date = self.text

            Label:
                text: "Course name"
                halign: "left"
                valign: "middle"
                text_size: self.size
            TextInput:
                text: root.course_name
                hint_text: "e.g., Pharmacy 25-26"
                multiline: False
                write_tab: False
                on_text: root.course_name = self.text

            Label:
                text: "Period name"
                halign: "left" 
                valign: "middle"
                text_size: self.size
            TextInput:
                text: root.period_name
                hint_text: "e.g., 1st Semester"
                multiline: False
                write_tab: False
                on_text: root.period_name = self.text

        Label:
            text: root.error_text
            color: 1, 0, 0, 1
            size_hint_y: None
            height: "18dp"

        BoxLayout:
            size_hint_y: None
            height: "48dp"
            spacing: 8

            Widget:
            Button:
                text: "Cancel"
                on_release: root.dismiss()
            Button:
                text: "OK"
                on_release: root.handle_ok()
'''

class AddPeriodPopup(Popup):
    start_date  = StringProperty("")
    course_name = StringProperty("")
    period_name = StringProperty("")
    # optional callback: def on_submit(payload: dict): ...
    on_submit   = ObjectProperty(allownone=True)

    # simple error message slot
    error_text  = StringProperty("")

    def __init__(
            self, fresh_start=False, 
            on_submit=None, start_date="", 
            course_name="", period_name="", 
            **kwargs):
        super().__init__(**kwargs)
        self.on_submit = on_submit
        self.start_date = start_date
        self.course_name = course_name
        self.period_name = period_name

    def validate(self):
        if not self.start_date.strip():
            return False, "Start date is required."
        if not self.course_name.strip():
            return False, "Course name is required."
        if not self.period_name.strip():
            return False, "Period name is required."
        return True, ""
    
    def handle_ok(self):
        ok, msg = self.validate()
        if not ok:
            self.error_text = msg
            return
        
        payload = {
            "start_date": self.start_date.strip(),
            "course_name": self.course_name.strip(),
            "period_name": self.period_name.strip(),
        }

        if callable(self.on_submit):
            self.on_submit(payload)

        self.dismiss()

Builder.load_string(AddPeriodPopupKv)

# --------------------------------------------------------

ExcludeSubjectsPopupKv = '''
<ExcludeSubjectsPopup>:
    title: "Select Subjects"
    size_hint: 0.85, 0.6
    auto_dismiss: True

    BoxLayout:
        orientation: "vertical"
        padding: 16
        spacing: 12

        Label:
            id: instructions
            text: root.dialog_title
            halign: "left"
            valign: "middle"
            size_hint_y: None
            height: self.texture_size[1]
            text_size: self.width, None

        ScrollView:
            do_scroll_x: False
            do_scroll_y: True

            GridLayout:
                id: subjects_grid
                cols: 1
                size_hint_y: None
                height: self.minimum_height
                row_default_height: "40dp"
                row_force_default: True
                spacing: 6
                padding: 0, 0

        BoxLayout:
            size_hint_y: None
            height: "48dp"
            spacing: 8

            Widget:
            Button:
                text: "Cancel"
                on_release: root.dismiss()
            Button:
                text: "OK"
                on_release: root.handle_ok()
'''

class ExcludeSubjectsPopup(Popup):
    dialog_title = StringProperty("Select subjects to exclude")

    def __init__(
            self, cnfg_mng,
            subjects_list:list=None, 
            db_mng=None,
            on_submit=None,
            course_name=None, period_name=None,
            **kwargs,
            ):
        
        super().__init__(**kwargs)
        self.on_submit = on_submit
        self.cnfg_mng  = cnfg_mng

        self.default_exclude = None
        if course_name is None and period_name is None:
            config = cnfg_mng().load_json_config()["current_period_data"]
            self.course_name=config["current_course"]
            self.period_name=config["current_period"]
            self.default_exclude = config.get("default_exclude", None)

        else:
            self.course_name = course_name
            self.period_name = period_name

        if subjects_list is None:
            self.subjects_list = db_mng().get_subjects(self.course_name, self.period_name)

        else:
            self.subjects_list = subjects_list
        
        # self.subjects_list = [s for s in self.subjects_list if s not in self.default_exclude]
        log.debug(f"Subject list={self.subjects_list}")

        self.dialog_title = f"Exclude subjects from {self.course_name} - {self.period_name}"
        
        self._build_subject_list()

    def _build_subject_list(self):
        grid = self.ids.get("subjects_grid")
        if not grid:
            return
        grid.clear_widgets()
        for subj in sorted(set(map(str, self.subjects_list))):
            btn = ToggleButton(
                text=subj,
                size_hint_y=None,
                height="40dp",
                # No 'group' -> multi-select allowed
                state="normal"  # start unselected
            )
            grid.add_widget(btn)

    def handle_cancel(self):
        if callable(self.on_submit):
            self.on_submit(None)
        self.dismiss()
    
    def _collect_selected_subjects(self) -> list[str]:
        grid = self.ids.get("subjects_grid")
        if not grid:
            return []
        selected = [child.text for child in grid.children if isinstance(child, ToggleButton) and child.state == "down"]
        return list(reversed(selected))
    
    def handle_ok(self):
        selected = self._collect_selected_subjects()
        payload = selected if selected else None
        
        if payload: self.cnfg_mng().change_exclude_list(payload)
        
        if callable(self.on_submit):
            # Behaviour is going to be contained in this popup, but im still leaving the payload here for debug purposes.
            self.on_submit(payload)
            
        self.dismiss()

Builder.load_string(ExcludeSubjectsPopupKv)
