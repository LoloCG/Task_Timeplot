from kivy.uix.popup import Popup
from kivy.lang import Builder
from kivy.properties import StringProperty, ObjectProperty

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
                hint_text: "e.g., 2025-09-13"
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

    def __init__(self, on_submit=None, start_date="", course_name="", period_name="", **kwargs):
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
