from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QHBoxLayout,
    QPushButton, QVBoxLayout, QFileDialog, QLabel, 
    QComboBox, QDateEdit,QTableWidget,QTableWidgetItem,
    QSizePolicy,QHeaderView,QScrollArea,QFrame
)
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from pathlib import Path
import pandas as pd
import datetime
from PyQt6.QtCore import (QDate,Qt,QTimer, pyqtSignal, QObject,QThread)

from PyQt6.QtGui import QColor, QPalette, QFont
import threading
from pyngrok import ngrok
import backend_server  
import Process_data
global global_folder_path
global_folder_path = None

class Worker(QObject):
    finished = pyqtSignal()
    error = pyqtSignal(str)

    def __init__(self,folder):
        super().__init__()
        self.folder = folder

    def run_process_data(self):
        try:
            Process_data.run(self.folder)
        except Exception as e:
            self.error.emit(str(e))
        self.finished.emit()
    def run_est_sch(self):
        try:
            Process_data.run_schdule(self.folder)
        except Exception as e:
            self.error.emit(str(e))
        self.finished.emit()
    def run_process_Arr_data(self):
        try:
            Process_data.run_A(self.folder)
        except Exception as e:
            self.error.emit(str(e))
        self.finished.emit()
    def run_schedule_A(self):
        try:
            Process_data.run_schduleA(self.folder)
        except Exception as e:
            self.error.emit(str(e))
        self.finished.emit()

class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self.server_thread = None
        self.server_running = False

        self.setWindowTitle("Suez Canal Manager")
        self.setWindowState(Qt.WindowState.WindowMaximized)

        app_font = QFont("Segoe UI", 12)
        self.setFont(app_font)

        main_container = QWidget()
        main_container.setSizePolicy(
            QSizePolicy.Policy.Expanding,
            QSizePolicy.Policy.Expanding
        )

        self.setStyleSheet("""QLabel {
                                font-size: 16px;
                                font-weight: 600;
                                color: #1e3d59;   /* Deep professional blue */
                            }""")
        self.main_layout = QVBoxLayout(main_container)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)

        nav_bar = QWidget()
        nav_bar.setFixedHeight(70)
        nav_bar.setStyleSheet("""
            background: rgba(0,0,0,120);
            border-bottom-left-radius: 15px;
            border-bottom-right-radius: 15px;
        """)

        nav_layout = QHBoxLayout()
        nav_layout.setContentsMargins(20, 10, 20, 10)
        nav_layout.setSpacing(20)

        self.btn_style = """
            QPushButton {
                background-color: #2b2b2b;
                color: #EDEDED;
                padding: 12px 28px;
                border: none;
                border-radius: 12px;
                font-size: 16px;
            }
            QPushButton:hover {
                background-color: #4b5c6d;
            }
        """

        btn_home = QPushButton("Home")
        btn_home.setStyleSheet(self.btn_style + "border-bottom-left-radius: 15px;")
        btn_home.clicked.connect(self.show_home_page)

        btn_actions = QPushButton("Actions")
        btn_actions.setStyleSheet(self.btn_style)
        btn_actions.clicked.connect(self.show_action_page)

        btn_view = QPushButton("View Data")
        btn_view.setStyleSheet(self.btn_style)
        btn_view.clicked.connect(self.show_view_page)

        btn_schedule = QPushButton("Schedule")
        btn_schedule.setStyleSheet(self.btn_style)
        btn_schedule.clicked.connect(self.show_schedule_page)

        btn_analysis = QPushButton("Analysis")
        btn_analysis.setStyleSheet(self.btn_style + "border-bottom-right-radius: 15px;")
        btn_analysis.clicked.connect(self.show_analysis_page)

        nav_layout.addWidget(btn_home)
        nav_layout.addWidget(btn_actions)
        nav_layout.addWidget(btn_view)
        nav_layout.addWidget(btn_schedule)
        nav_layout.addWidget(btn_analysis)
        nav_bar.setLayout(nav_layout)

        self.main_layout.addWidget(nav_bar)

        self.content_area = QWidget()
        self.content_layout = QVBoxLayout(self.content_area)

        
        self.content_area.setLayout(self.content_layout)
        self.content_area.setStyleSheet("background-color: #BDBDBD;")
        self.main_layout.addWidget(self.content_area,1)

        self.status_label = QLabel("Server Status: Not Running")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.status_label.setStyleSheet("""
            font-size: 20px;
            font-weight: 600;
            color: #222;
            padding: 8px;
            background-color: #d0d0d0;
            border-top: 2px solid #aaa;
        """)
        self.main_layout.addWidget(self.status_label,0)

        
        self.setCentralWidget(main_container)



        self.show_home_page()


    def show_home_page(self):
        for i in reversed(range(self.content_layout.count() )):
            widget = self.content_layout.itemAt(i).widget()
            if widget:
                widget.deleteLater()

        home_page = QWidget()
        layout = QVBoxLayout()
        layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        layout.setContentsMargins(50, 40, 50, 40)
        layout.setSpacing(35)

        self.file_label = QLabel("Selected Folder: (none)")
        self.file_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.file_label.setStyleSheet("font-size:18px; color: #1a1a1a; font-weight:500;")
        self.file_label.setText(f"Selected Folder: {global_folder_path}")

        browse_btn = QPushButton("Select Folder")
        browse_btn.setFixedSize(250, 80)
        browse_btn.setStyleSheet(self.btn_style)
        browse_btn.clicked.connect(self.pick_folder)


        start_btn = QPushButton("Start Server")
        start_btn.setFixedSize(250, 80)
        start_btn.setStyleSheet(self.btn_style)
        start_btn.clicked.connect(self.start_server)

        stop_btn = QPushButton("Stop Server")
        stop_btn.setFixedSize(250, 80)
        stop_btn.setStyleSheet(self.btn_style)
        stop_btn.clicked.connect(self.stop_server)

        btn_row = QHBoxLayout()
        btn_row.setAlignment(Qt.AlignmentFlag.AlignCenter)
        btn_row.setSpacing(30)
        btn_row.addWidget(start_btn)
        btn_row.addWidget(stop_btn)

        layout.addWidget(browse_btn, alignment=Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.file_label)
        layout.addLayout(btn_row)

        home_page.setLayout(layout)
        self.content_layout.insertWidget(0, home_page)

  
    def pick_folder(self,cond=0):
        global global_folder_path
        folder = QFileDialog.getExistingDirectory(self, "Select Main Folder")
        if folder:
            global_folder_path = folder
            backend_server.excel_file = folder + "/form_data.xlsx"
            self.file_label.setText(f"Selected Folder: {folder}")
        if(cond==2):
            self.update_type_options()
            self.load_table_data()
        if(cond ==3):
            self.load_sch_data()
        if(cond ==4):
            self.refresh_analysis()

    def start_server(self):
        if self.server_running:
            self.status_label.setText("Server already running.")
            return

        if backend_server.excel_file is None:
            self.status_label.setText("⚠ Select a folder first.")
            return

        def run_backend():
            backend_server.start_flask()

        self.server_thread = threading.Thread(target=run_backend, daemon=True)
        self.server_thread.start()
        self.server_running = True

        self.status_label.setText("Starting server...")

        self.url_checker = QTimer()
        self.url_checker.setInterval(500)  

        def check_url():
            if backend_server.public_url:
                self.status_label.setText(f"Server Running at: {backend_server.public_url}")
                self.url_checker.stop()

        self.url_checker.timeout.connect(check_url)
        self.url_checker.start()

    def stop_server(self):
        if not self.server_running:
            self.status_label.setText("Server is not running.")
            return

        try:
            ngrok.kill()
        except:
            pass

        self.server_running = False
        self.status_label.setText("Server Stopped")
    
    def show_action_page(self):
        for i in reversed(range(self.content_layout.count())):
            widget = self.content_layout.itemAt(i).widget()
            if widget:
                widget.deleteLater()
        action_page = QWidget()
        grid = QHBoxLayout()
        grid.setContentsMargins(80, 60, 80, 60)
        grid.setSpacing(60)

        left_col = QVBoxLayout()
        left_col.setSpacing(25)

        right_col = QVBoxLayout()
        right_col.setSpacing(25)

        self.file_label = QLabel("Selected Folder: (none)")
        self.file_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.file_label.setStyleSheet("font-size:18px; color: #1a1a1a; font-weight:500;")
        self.file_label.setText(f"Selected Folder: {global_folder_path}")

        browse_btn = QPushButton("Select Folder")
        browse_btn.setStyleSheet(self.btn_style)
        browse_btn.setFixedHeight(70)
        browse_btn.clicked.connect(self.pick_folder)

        left_col.addWidget(browse_btn)
        right_col.addWidget(self.file_label)

   
        def add_row(btn_text, on_click=None):
            btn = QPushButton(btn_text)
            btn.setStyleSheet(self.btn_style)
            btn.setFixedHeight(70)

            lbl = QLabel("Waiting…")

            if(global_folder_path == None):
                lbl.setText("⚠ No folder selected!")
            lbl.setStyleSheet("font-size: 17px; font-weight: 600; color: #333;")
            lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)

            def clicked():
                if(global_folder_path == None):
                    lbl.setText("⚠ No folder selected!")
                else:    
                    start_running_animation(self)

                    if on_click:
                        btn.setEnabled(False)
                        self.thread = QThread()
                        self.worker = Worker(global_folder_path)

                        self.worker.moveToThread(self.thread)
                        if(on_click == 1):
                            self.thread.started.connect(self.worker.run_process_data)
                        elif(on_click == 2):
                            self.thread.started.connect(self.worker.run_est_sch)
                        elif(on_click == 3):
                            self.thread.started.connect(self.worker.run_process_Arr_data)
                        elif(on_click == 4):
                            self.thread.started.connect(self.worker.run_schedule_A)
                        self.worker.finished.connect(self.thread.quit)

                        self.worker.finished.connect(lambda: on_task_finished())
                        self.worker.error.connect(lambda e: lbl.setText(f"❌ {e}"))

                        self.thread.start()
                    else:
                        lbl.setText("✔ Done")
                        lbl.setText("DONE")
                        lbl.setStyleSheet("""
                            QLabel {
                                background-color: #e8f5e9;
                                color: #2e7d32;
                                border-radius: 12px;
                                padding: 6px 18px;
                                font-size: 14px;
                                font-weight: 800;
                            }
                        """)
            def start_running_animation(self):
                self._dots = 0
                self._pulse = True

                self._timer = QTimer()
                self._timer.timeout.connect(lambda: animate_status(self))
                self._timer.start(450)
            def animate_status(self):
                self._dots = (self._dots + 1) % 4
                dots = "." * self._dots

                color = "#d97706" if self._pulse else "#f59e0b"  # amber pulse
                self._pulse = not self._pulse

                lbl.setText(f"⏳ Processing{dots}")
                lbl.setStyleSheet(f"""
                    font-size: 17px;
                    font-weight: 600;
                    color: {color};
                """)


            def on_task_finished():
                #lbl.setText("✔ Done")
                
                self._timer.stop()

                lbl.setText("DONE")
                lbl.setStyleSheet("""
                    QLabel {
                        background-color: #e8f5e9;
                        color: #2e7d32;
                        border-radius: 12px;
                        padding: 6px 18px;
                        font-size: 14px;
                        font-weight: 800;
                    }
                """)
                btn.setEnabled(True)

            btn.clicked.connect(clicked)

            left_col.addWidget(btn)
            right_col.addWidget(lbl)
            
        add_row("Process New Data",on_click = 1)
        add_row("Estimated Schedule", on_click=2)
        add_row("Process Today Data",on_click=3)
        add_row("Today Schedule",on_click=4)

        grid.addLayout(left_col, 1)
        grid.addLayout(right_col, 1)

        action_page.setLayout(grid)

        self.content_layout.insertWidget(0, action_page)

    def show_view_page(self):
        for i in reversed(range(self.content_layout.count())):
            widget = self.content_layout.itemAt(i).widget()
            if widget:
                widget.deleteLater()

        self.view_page = QWidget()
        main_layout = QVBoxLayout(self.view_page)
        self.view_page.setSizePolicy(
            QSizePolicy.Policy.Expanding,
            QSizePolicy.Policy.Expanding
        )

        browse = QWidget()
        browse_layout = QHBoxLayout(browse)
        browse.setMaximumHeight(60)
 
        browse_btn = QPushButton("Select Folder")
        browse_btn.setStyleSheet(self.btn_style)
        browse_btn.setFixedHeight(45)
        browse_btn.clicked.connect(lambda: self.pick_folder(2))

        browse_layout.addWidget(browse_btn)
        browse_layout.addWidget(self.file_label)
        
        self.arrival_status = QComboBox()
        self.arrival_status.setStyleSheet("""QComboBox {
                                                font-size: 15px;
                                                padding: 6px 10px;
                                                border-radius: 6px;
                                                border: 1px solid #b0b8c3;
                                                background: #ffffff;
                                                color: #263238;
                                            }

                                            QComboBox:hover {
                                                border: 1px solid #4682b4;  /* Steel blue on hover */
                                            }
                                            """)
        self.arrival_status.addItems(["Before Arrival", "After Arrival"])

        self.type_of_data = QComboBox()
        self.type_of_data.setStyleSheet("""QComboBox {
                                        font-size: 15px;
                                        padding: 6px 10px;
                                        border-radius: 6px;
                                        border: 1px solid #b0b8c3;
                                        background: #ffffff;
                                        color: #263238;
                                    }

                                    QComboBox:hover {
                                        border: 1px solid #4682b4;  /* Steel blue on hover */
                                    }
                                    """)

        self.date_field = QDateEdit()
        self.date_field.setStyleSheet("""QDateEdit {
                                            font-size: 15px;
                                            padding: 6px 10px;
                                            border-radius: 6px;
                                            border: 1px solid #b0b8c3;
                                            background: #ffffff;
                                            color: #263238;
                                        }

                                        QDateEdit:hover {
                                            border: 1px solid #4682b4;
                                        }
                                      
                                        /* ---------- DateEdit DISABLED ---------- */
                                        QDateEdit:disabled {
                                            background: #f0f2f5;
                                            border: 1px solid #d1d5db;
                                            color: #8a9099;

                                            /* Prevent calendar popup icon from being colored weird */
                                        }

                                        QDateEdit::drop-down:disabled {
                                            background: #e4e7eb;
                                        }

                                        QDateEdit::down-arrow:disabled {
                                            image: none;
                                        }""")
        self.date_field.setCalendarPopup(True)
        self.date_field.setDate(QDate.currentDate())
        self.date_field.setEnabled(False)
        filter_bar = QWidget()

        label1 = QLabel("Arrival Status")
        label2 = QLabel("Type of Data")
        label3 = QLabel("Date")

        layout = QHBoxLayout(filter_bar)
        layout.addWidget(label1)
        layout.addWidget(self.arrival_status)

        layout.addWidget(label2)
        layout.addWidget(self.type_of_data)

        layout.addWidget(label3)
        layout.addWidget(self.date_field)
        main_layout.addWidget(browse)
        main_layout.addWidget(filter_bar)

        self.table = QTableWidget()
        self.table.setObjectName("dataTable")
        self.table.setStyleSheet("QTableWidget { font-size: 14px; }")
   
        self.table.setSizePolicy(
            QSizePolicy.Policy.Expanding,
            QSizePolicy.Policy.Expanding
        )
     
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)

        self.table.setAlternatingRowColors(True)
        self.table.setMinimumHeight(350)
        self.table.setMaximumHeight(500)
        self.table.setStyleSheet("""QTableWidget {
                                        background: #FFFFFF;
                                        color: #2B2B2B;
                                        font-size: 14px;
                                        border-radius: 8px;
                                        border: 1px solid #C4C4C4;
                                        gridline-color: #DCDCDC;
                                        selection-background-color: #006CBF;
                                        selection-color: #FFFFFF;
                                        alternate-background-color: #F2F2F2;
                                    }

                                    QHeaderView::section {
                                        background: #F4F4F4;
                                        color: #2E2E2E;
                                        padding: 7px;
                                        border-bottom: 1px solid #D0D0D0;
                                        font-weight: 600;
                                    }

                                    QScrollBar:vertical {
                                        background: #000000;
                                        width: 10px;
                                    }
                                    QScrollBar::handle:vertical {
                                        background: #999999;
                                        border-radius: 5px;
                                    }
                                """)


        main_layout.addWidget(self.table,1)
        self.content_layout.addWidget(self.view_page,1)


        self.arrival_status.currentIndexChanged.connect(self.update_type_options)
        self.type_of_data.currentIndexChanged.connect(self.update_date_field)
        self.type_of_data.currentIndexChanged.connect(self.load_table_data)
        self.arrival_status.currentIndexChanged.connect(self.load_table_data)
        self.date_field.dateChanged.connect(self.load_table_data)

        self.update_type_options()
        self.load_table_data()

    def update_type_options(self):
        selected = self.arrival_status.currentText()
        self.type_of_data.clear()

        if selected == "Before Arrival":
            self.type_of_data.addItems([
                "Selected Vessel",
                "Rejected Vessel",
                "Vessel Info",
                "Schedule By Date"
            ])
        else:  
            self.type_of_data.addItems([
                "On time ships",
                "Late ships"
            ])

        self.update_date_field()


    def update_date_field(self):
        text = self.type_of_data.currentText()

        if text in ["Vessel Info","Schedule By Date", "On time ships", "Late ships"]:
            self.date_field.setEnabled(True)
        else:
            self.date_field.setEnabled(False)

    
    def load_table_data(self):
        try:
            folder = global_folder_path
            if self.arrival_status.currentText() == "Before Arrival":
                selected_type = self.type_of_data.currentText()
                if(selected_type == "Selected Vessel"):
                    excel_path = folder + "/CLEAN_DATA/BeforeArrival/BA_CLEAN.xlsx"
                elif(selected_type == "Rejected Vessel"):
                    excel_path = folder + "/CLEAN_DATA/BeforeArrival/NNBA_CLEAN.xlsx"
                elif(selected_type == "Vessel Info"):
                    selected_day = self.date_field.date().toPyDate()
                    excel_path = folder + "/PROCESSED_DATA/BeforeArrival/"+str(selected_day)+".xlsx"
                elif(selected_type == "Schedule By Date"):
                    selected_day = self.date_field.date().toPyDate()
                    excel_path = folder + "/SCH_DATA/Before_Arrival/"+str(selected_day)+".xlsx"
                
                
            else:
                selected_day = self.date_field.date().toPyDate()
                selected_type = self.type_of_data.currentText()
                if(selected_type == "On time ships"):
                    excel_path = folder + "/PROCESSED_DATA/AfterArrival/OnTime/"+str(selected_day)+".xlsx"
                elif(selected_type == "Late ships"):
                    excel_path = folder + "/PROCESSED_DATA/AfterArrival/Late/"+str(selected_day)+".xlsx"
            df = pd.read_excel(excel_path)

            self.display_dataframe(df)

        except Exception as e:
            print("Load Error:", e)
            df = pd.DataFrame(columns=["No Data Exist"])
            self.display_dataframe(df)

    def display_dataframe(self, df):
        self.table.clear()
        self.table.setColumnCount(len(df.columns))
        self.table.setRowCount(len(df))
        self.table.setHorizontalHeaderLabels(df.columns)

        for row in range(len(df)):
            for col in range(len(df.columns)):
                value = str(df.iat[row, col])
                self.table.setItem(row, col, QTableWidgetItem(value))

        self.table.resizeColumnsToContents()
        self.table.resizeRowsToContents()


    def show_schedule_page(self):
        for i in reversed(range(self.content_layout.count())):
            widget = self.content_layout.itemAt(i).widget()
            if widget:
                widget.deleteLater()

        self.schedule_page = QWidget()
        main_layout = QVBoxLayout(self.schedule_page)
        main_layout.setContentsMargins(30, 30, 30, 30)
        main_layout.setSpacing(20)

        controls_widget = QWidget()
        controls_layout = QHBoxLayout(controls_widget)
        controls_layout.setContentsMargins(0, 0, 0, 0)
        controls_layout.setSpacing(20)

        browse_btn = QPushButton("Select Folder")
        browse_btn.setStyleSheet("""
            QPushButton {
                background-color: #2b2b2b;
                color: #EDEDED;
                padding: 12px 28px;
                border: none;
                border-radius: 12px;
                font-size: 16px;
            }
            QPushButton:hover {
                background-color: #4b5c6d;
            }
        """)
        browse_btn.setFixedSize(200, 45)
        browse_btn.clicked.connect(lambda: self.pick_folder(3))
        controls_layout.addWidget(browse_btn, 0, Qt.AlignmentFlag.AlignVCenter)

        self.date_picker = QDateEdit()
        self.date_picker.setCalendarPopup(True)
        self.date_picker.setDate(QDate.currentDate())
        self.date_picker.setFixedHeight(45)
        self.date_picker.setFixedWidth(160)
        self.date_picker.setStyleSheet("""
            QDateEdit {
                font-size: 15px;
                padding: 6px 10px;
                border-radius: 6px;
                border: 1px solid #b0b8c3;
                background: #ffffff;
                color: #263238;
            }
            QDateEdit:hover {
                border: 1px solid #4682b4;
            }
            QDateEdit:disabled {
                background: #f0f2f5;
                border: 1px solid #d1d5db;
                color: #8a9099;
            }
            QDateEdit::drop-down:disabled { background: #e4e7eb; }
            QDateEdit::down-arrow:disabled { image: none; }
        """)
        controls_layout.addWidget(self.date_picker, 0, Qt.AlignmentFlag.AlignVCenter)

        if not hasattr(self, 'file_label'):
            self.file_label = QLabel("No folder selected")
        self.file_label.setStyleSheet("color: white; font-size: 14px;")
        self.file_label.setWordWrap(True)
        self.file_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        controls_layout.addWidget(self.file_label, 1)

        main_layout.addWidget(controls_widget)

        compare_container = QWidget()
        compare_layout = QVBoxLayout(compare_container)
        compare_layout.setSpacing(25)
        compare_layout.setContentsMargins(0, 0, 0, 0)

        self.north_widget = self.create_direction_compare("Northbound")
        self.south_widget = self.create_direction_compare("Southbound")

        compare_layout.addWidget(self.north_widget, 1)
        compare_layout.addWidget(self.south_widget, 1)

        main_layout.addWidget(compare_container, 1)


        self.content_layout.addWidget(self.schedule_page, 1)

        self.date_picker.dateChanged.connect(self.load_sch_data)
        try:
            self.date_picker.setDate(self.select_day_sch)
        except :
            print("Load Error: Date Not Set ")
        self.load_sch_data();
    def create_direction_compare(self, title_text):
        card = QWidget()
        card.setStyleSheet("""
            QWidget {
                background-color: #1f1f1f;
                border-radius: 14px;
                border: 1px solid #333;
            }
        """)

        layout = QVBoxLayout(card)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(16)

        title = QLabel(title_text.upper())
        title.setStyleSheet("""
            font-size: 20px;
            font-weight: 700;
            color: #EDEDED;
            letter-spacing: 1px;
        """)
        layout.addWidget(title)

        head = self.create_timeline_head()
        est, est_nodes = self.create_timeline_row("Estimated")
        act, act_nodes = self.create_timeline_row("Actual")

        layout.addWidget(head)
        layout.addWidget(est)
        layout.addWidget(act)
        if title_text == "Northbound":
            self.north_nodes_est = est_nodes
            self.north_nodes_act = act_nodes
        else:
            self.south_nodes_est = est_nodes
            self.south_nodes_act = act_nodes

        return card
    def create_timeline_head(self):
        row = QWidget()
        row_layout = QHBoxLayout(row)
        row_layout.setContentsMargins(0, 0, 0, 0)
        row_layout.setSpacing(14)

        label = QLabel("Time ->")
        label.setFixedWidth(90)
        label.setStyleSheet("""
            color: #BDBDBD;
            font-size: 14px;
            font-weight: 600;
        """)
        row_layout.addWidget(label)

        timeline = QWidget()
        timeline_layout = QHBoxLayout(timeline)
        timeline_layout.setSpacing(18)

        nodes = {}
        for name in [
            "Arrival Time",
            "Start Time",
            "Dual Started Time",
            "Waited Time",
            "Dual Reached Time",
            "Destitation Time"
        ]:
            node = QLabel(name)
            node.setToolTip(name)
            node.setAlignment(Qt.AlignmentFlag.AlignCenter)
            node.setStyleSheet("""
                QLabel {
                    font-size: 18px;
                    color: #4CAF50;
                }
            """)
            timeline_layout.addWidget(node)
            nodes[name] = node

        row_layout.addWidget(timeline, 1)
        return row
    def create_timeline_row(self, label_text):
        row = QWidget()
        row_layout = QHBoxLayout(row)
        row_layout.setContentsMargins(0, 0, 0, 0)
        row_layout.setSpacing(14)

        label = QLabel(label_text)
        label.setFixedWidth(90)
        label.setStyleSheet("""
            color: #BDBDBD;
            font-size: 14px;
            font-weight: 600;
        """)
        row_layout.addWidget(label)

        timeline = QWidget()
        timeline_layout = QHBoxLayout(timeline)
        timeline_layout.setSpacing(18)

        nodes = {}
        for name in [
            "Scheduled",
            "Start",
            "Before",
            "Wait",
            "Dual",
            "Dest"
        ]:
            node = QLabel("●")
            node.setToolTip(name)
            node.setAlignment(Qt.AlignmentFlag.AlignCenter)
            node.setStyleSheet("""
                QLabel {
                    font-size: 18px;
                    color: #4CAF50;
                }
            """)
            timeline_layout.addWidget(node)
            nodes[name] = node

        row_layout.addWidget(timeline, 1)
        return row, nodes

    def load_sch_data(self):
        if(global_folder_path == None):
            return
        self.select_day_sch = self.date_picker.date().toPyDate()
        try:
            folder = global_folder_path
            selected_day = self.date_picker.date().toPyDate()
            before_excel_path = folder + "/SCH_DATA/Before_Arrival/"+str(selected_day)+".xlsx"

            b_df = pd.read_excel(before_excel_path)
        except Exception as e:
            print("Load Error:", e)
            b_df = pd.DataFrame(columns=["Direction"])
           

        try:
            folder = global_folder_path
            selected_day = self.date_picker.date().toPyDate()
            after_excel_path = folder + "/SCH_DATA/After_Arrival/"+str(selected_day)+".xlsx"
       

            a_df = pd.read_excel(after_excel_path)
        except Exception as e:
            print("Load Error:", e)
            a_df = pd.DataFrame(columns=["Direction"])

        est_north = b_df[b_df["Direction"] == "Northbound"]
        act_north = a_df[a_df["Direction"] == "Northbound"]

        est_south = b_df[b_df["Direction"] == "Southbound"]
        act_south = a_df[a_df["Direction"] == "Southbound"]

        self.update_direction("Northbound", est_north, act_north)
        self.update_direction("Southbound", est_south, act_south)
 
    def update_direction(self, direction, est_df, act_df):

        def to_time(val):
            if pd.isna(val):
                return pd.NaT

            if isinstance(val, (pd.Timestamp, datetime.datetime)):
                return val

            if isinstance(val, datetime.time):
                return pd.Timestamp.combine(pd.Timestamp.today(), val)

            val = str(val).strip()

            for fmt in ("%H:%M", "%H:%M:%S"):
                try:
                    return pd.to_datetime(val, format=fmt)
                except ValueError:
                    pass

            return pd.NaT


        def color_for_delta(delta):
            if pd.isna(delta):
                return "#9E9E9E" 
            if delta > pd.Timedelta(minutes=15):
                return "#FF9800"  
            if delta < pd.Timedelta(minutes=-5):
                return "#03A9F4"  
            return "#4CAF50"     
        est_status = 1
        act_status = 1

        if direction == "Northbound":
            act_nodes = self.north_nodes_act
            est_nodes = self.north_nodes_est
        else:
            act_nodes = self.south_nodes_act
            est_nodes = self.south_nodes_est

        if est_df.empty :
            est_status = 0
        else: 
            est = est_df.iloc[0]
                
        if act_df.empty:
            act_status = 0
        else:
            act = act_df.iloc[0]
           
        fields = [
            ("Scheduled", "Scheduled Time"),
            ("Start", "StartTime"),
            ("Before", "Before_section"),
            ("Wait", "Wait Time"),
            ("Dual", "Dual Crossed Time"),
            ("Dest", "DestTime"),
        ]
        if(est_status == 1 and act_status == 1):
            for key, col in fields:
                est_time = to_time(est[col])
                act_time = to_time(act[col])
                
                delta = act_time - est_time
                color = color_for_delta(delta)

                est_nodes[key].setText(str(est[col]))
                est_nodes[key].setStyleSheet(
                    "font-size:18px; color:#BDBDBD;"
                )

                act_nodes[key].setText(str(act[col]))
                act_nodes[key].setStyleSheet(
                    f"font-size:18px; background-color:{color};"
                )
        elif(est_status == 1):
            for key, col in fields:
                est_time = to_time(est[col])

                color = color_for_delta(pd.NaT)

                est_nodes[key].setText(str(est[col]))
                est_nodes[key].setStyleSheet(
                    "font-size:18px; color:#BDBDBD;"
                )

                act_nodes[key].setText("●")
                act_nodes[key].setStyleSheet(
                    f"font-size:18px; color:{color};background-color: #1f1f1f;"
                )
        elif(act_status ==1):
            for key, col in fields:
                act_time = to_time(act[col])
                color = color_for_delta(pd.NaT)

                est_nodes[key].setText("●")
                est_nodes[key].setStyleSheet(
                    "font-size:18px; color:#BDBDBD;"
                )

                act_nodes[key].setText(str(act[col]))
                act_nodes[key].setStyleSheet(
                    f"font-size:18px; color:{color};background-color: #1f1f1f;"
                )
        else:
            for key, col in fields:
                color = color_for_delta(pd.NaT)

                est_nodes[key].setText("●")
                est_nodes[key].setStyleSheet(
                    "font-size:18px; color:#BDBDBD;background-color: #1f1f1f;"
                )

                act_nodes[key].setText("●")
                act_nodes[key].setStyleSheet(
                    f"font-size:18px; color:{color};background-color: #1f1f1f;"
                )






    def display_dataframe_sch(self, df):
        self.sch_section.clear()
        self.sch_section.setColumnCount(len(df.columns))
        self.sch_section.setRowCount(len(df))
        self.sch_section.setHorizontalHeaderLabels(df.columns)

        for row in range(len(df)):
            for col in range(len(df.columns)):
                value = str(df.iat[row, col])
                self.sch_section.setItem(row, col, QTableWidgetItem(value))

        self.sch_section.resizeColumnsToContents()
        self.sch_section.resizeRowsToContents()
    def show_analysis_page(self):
        for i in reversed(range(self.content_layout.count())):
            widget = self.content_layout.itemAt(i).widget()
            if widget:
                widget.deleteLater()

        self.analysis_page = QWidget()
        main_layout = QVBoxLayout(self.analysis_page)
        main_layout.setContentsMargins(30, 30, 30, 30)
        main_layout.setSpacing(20)

        top_bar = QWidget()
        top_layout = QHBoxLayout(top_bar)
        top_layout.setSpacing(15)

        browse_btn = QPushButton("Select Folder")
        browse_btn.setStyleSheet(self.btn_style)
        browse_btn.setFixedHeight(45)
        browse_btn.clicked.connect(lambda: self.pick_folder(4))

        top_layout.addWidget(browse_btn)

        top_layout.addWidget(self.file_label, 1)

        main_layout.addWidget(top_bar)

        filter_bar = QWidget()
        filter_layout = QHBoxLayout(filter_bar)
        filter_layout.setSpacing(15)

        label1 = QLabel("Analysis On")
        label2 = QLabel("Date")

        self.time_analysis = QComboBox()
        self.time_analysis.addItems(["Day", "All"])
        self.time_analysis.setFixedWidth(120)

        self.date_field_ana = QDateEdit()
        self.date_field_ana.setCalendarPopup(True)
        self.date_field_ana.setDate(QDate.currentDate())
        self.date_field_ana.setEnabled(True)

        filter_layout.addWidget(label1)
        filter_layout.addWidget(self.time_analysis)
        filter_layout.addWidget(label2)
        filter_layout.addWidget(self.date_field_ana)
        filter_layout.addStretch()

        main_layout.addWidget(filter_bar)

        self.analysis_output = QWidget()
        self.analysis_layout = QVBoxLayout(self.analysis_output)
        self.analysis_layout.setSpacing(30)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)

        self.analysis_output = QWidget()
        self.analysis_layout = QVBoxLayout(self.analysis_output)
        self.analysis_layout.setSpacing(20)  
        self.analysis_layout.addStretch()    

        scroll.setWidget(self.analysis_output)
        main_layout.addWidget(scroll, 1)
        self.content_layout.addWidget(self.analysis_page, 1)

        self.time_analysis.currentTextChanged.connect(self.on_analysis_mode_change)
        self.date_field_ana.dateChanged.connect(self.run_day_analysis)

        try:
            
            self.date_field_ana.setDate(self.select_day_ana)
        except Exception as e:
            print("Load Error: Date Not Set ",e)

        try:
            self.time_analysis.setCurrentText(self.select_timeA_ana)
        except :
            print("Load Error: Date Not Set ")
        try:
            self.on_analysis_mode_change(self.select_timeA_ana)
        except:
            print("Loaded First Time")
    def run_all_analysis(self):
        """
        Runs COMPLETE analysis on full dataset
        """
        
        selected_date = self.date_field_ana.date().toPyDate()
        self.select_day_ana = self.date_field_ana.date().toPyDate()
        if(global_folder_path==None) or global_folder_path=="":
            return
        self.run_before_arrival_analysis()

        
        self.run_after_arrival_analysis()
    def run_day_analysis(self):

        selected_date = self.date_field_ana.date().toPyDate()
        self.select_day_ana = self.date_field_ana.date().toPyDate()
        if(global_folder_path==None) or global_folder_path=="":
            return
        self.run_after_arrival_analysis(day=selected_date)


    def refresh_analysis(self):
        if global_folder_path is None:
            return

        for i in reversed(range(self.analysis_layout.count())):
            w = self.analysis_layout.itemAt(i).widget()
            if w:
                w.deleteLater()

        mode = self.time_analysis.currentText()

        if mode == "All":
            self.run_all_analysis()
        else:
            self.run_day_analysis()

   
    def on_analysis_mode_change(self, text):
        self.select_timeA_ana = text
        self.date_field_ana.setEnabled(text == "Day")
        self.refresh_analysis()




    def run_before_arrival_analysis(self, day=None):
        base = Path(global_folder_path)
       
        accepted_path = base / "CLEAN_DATA/BeforeArrival/BA_CLEAN.xlsx"
        rejected_path = base / "CLEAN_DATA/BeforeArrival/NNBA_CLEAN.xlsx"

        cl_accepted_df = pd.read_excel(accepted_path)
        cl_rejected_df = pd.read_excel(rejected_path)

        total_accepted = len(cl_accepted_df)
        total_rejected = len(cl_rejected_df)

        accepted_dir = cl_accepted_df["Direction"].value_counts()
        rejected_dir = cl_rejected_df["Direction"].value_counts()

        summary_df = pd.DataFrame({
            "Accepted": accepted_dir,
            "Rejected": rejected_dir
        }).fillna(0).astype(int)

        self.plot_bar(
            ["Accepted", "Rejected"],
            [total_accepted, total_rejected],
            "Total Vessel Status"
        )

        self.plot_series(accepted_dir, "Accepted Vessels by Direction")
        self.plot_series(rejected_dir, "Rejected Vessels by Direction")
        self.plot_df(summary_df, "Accepted vs Rejected by Direction")
    def run_after_arrival_analysis(self,day=None):
        base = Path(global_folder_path) / "PROCESSED_DATA/AfterArrival"
        st_base = global_folder_path +"/PROCESSED_DATA/AfterArrival"
        ontime_files = list((base / "OnTime").glob("*.xlsx"))
        late_files   = list((base / "Late").glob("*.xlsx"))
        
        ontime_status = 0
        late_status = 0
        if day!=None:
            try:

                ontime_df = pd.read_excel(st_base + "/OnTime/"+str(day)+".xlsx")
            except Exception as e:
                print("Load error: ",e)
                ontime_status = 1
                
            try:

                late_df   = pd.read_excel(st_base + "/Late/"+str(day)+".xlsx")
            except Exception as e:
                print("Load error: ",e)
                late_status = 1
                
        else: 
            ontime_df = pd.concat([pd.read_excel(f) for f in ontime_files], ignore_index=True)
            late_df   = pd.concat([pd.read_excel(f) for f in late_files], ignore_index=True)
            
        if(ontime_status != 1):
            total_ontime = len(ontime_df)
        if(late_status != 1):
            total_late = len(late_df)
        if(ontime_status!=1 and late_status!=1):
            self.plot_bar(
                ["On-Time", "Late"],
                [total_ontime, total_late],
                "On-Time vs Late Vessels"
            )
        if(ontime_status!=1):
            self.plot_hist(ontime_df["Fine Amount"], "Fine Distribution", "Fine Amount")
            self.plot_scatter(
                ontime_df["Length (m)"],
                ontime_df["Diff_minutes"],
                "Ship Length vs Early",
                "Length (m)",
                "Early (Minutes)"
            )
            self.plot_scatter(
                ontime_df["Beam(Max Width) (m)"],
                ontime_df["Diff_minutes"],
                "Ship Length vs Early",
                "Length (m)",
                "Early (Minutes)"
            )
            self.plot_scatter(
                ontime_df["Draft(depth) (m)"],
                ontime_df["Diff_minutes"],
                "Ship Length vs Early",
                "Length (m)",
                "Early (Minutes)"
            )
        if(late_status != 1):
            self.plot_hist(late_df["Diff_minutes"], "Delay Distribution", "Delay (Minutes)")
            self.plot_hist(late_df["Fine Amount"], "Fine Distribution", "Fine Amount")

            self.plot_scatter(
                late_df["Length (m)"],
                late_df["Diff_minutes"],
                "Ship Length vs Delay",
                "Length (m)",
                "Delay (Minutes)"
            )
            self.plot_scatter(
                late_df["Beam(Max Width) (m)"],
                late_df["Diff_minutes"],
                "Ship Length vs Delay",
                "Length (m)",
                "Delay (Minutes)"
            )
            self.plot_scatter(
                late_df["Draft(depth) (m)"],
                late_df["Diff_minutes"],
                "Ship Length vs Delay",
                "Length (m)",
                "Delay (Minutes)"
            )


    def create_canvas(self):
        fig = Figure(figsize=(6, 4))
        canvas = FigureCanvas(fig)

        canvas.setMinimumHeight(260)
        canvas.setMaximumHeight(360)


        canvas.setSizePolicy(
            QSizePolicy.Policy.Expanding,
            QSizePolicy.Policy.Expanding
        )

        self.analysis_layout.addWidget(canvas)
        return fig.add_subplot(111), canvas



    def plot_bar(self, labels, values, title):
        ax, canvas = self.create_canvas()
        ax.bar(labels, values)
        ax.set_title(title)
        canvas.draw()

    def plot_series(self, series, title):
        ax, canvas = self.create_canvas()
        series.plot(kind="bar", ax=ax)
        ax.set_title(title)
        ax.tick_params(axis='x', rotation=0)
        canvas.draw()

    def plot_df(self, df, title):
        ax, canvas = self.create_canvas()
        df.plot(kind="bar", ax=ax)
        ax.set_title(title)
        ax.tick_params(axis='x', rotation=0)
        canvas.draw()

    def plot_hist(self, data, title, xlabel):
        ax, canvas = self.create_canvas()
        ax.hist(data.dropna(), bins=10)
        ax.set_title(title)
        ax.set_xlabel(xlabel)
        canvas.draw()

    def plot_scatter(self, x, y, title, xlabel, ylabel):
        ax, canvas = self.create_canvas()
        ax.scatter(x, y)
        ax.set_title(title)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        canvas.draw()

app = QApplication([])
window = MainWindow()

palette = window.palette()
palette.setColor(QPalette.ColorRole.Window, QColor("#BDBDBD"))
window.setPalette(palette)

window.show()
app.exec()
