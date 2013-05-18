#!/usr/bin/env python
# -*- coding: utf-8 -*-
from multiprocessing import Process

import sys
from PyQt4.QtCore import SIGNAL
from PyQt4.QtGui import QApplication, QMainWindow, QGridLayout, QWidget, \
    QPushButton, QLabel, QFileDialog, QLineEdit, QSpinBox

from process import *


class MainWindow(QMainWindow):
    def __init__(self):
        QMainWindow.__init__(self)
        self.setupGUI()

    def setupGUI(self):
        layout = QGridLayout()
        self.widget = QWidget()
        self.widget.setLayout(layout)
        self.setCentralWidget(self.widget)
        self.setWindowTitle("Zargan Network Analyzer")

        self.input_file_label = QLabel("Input File")
        self.input_file_edit = QLineEdit()
        self.browse_button = QPushButton("Browse")

        self.line_count_label = QLabel("First N lines")
        self.line_count_edit = QLineEdit()
        self.line_count_edit.setText("500000")

        self.window_size_label = QLabel("Window Size (seconds)")
        self.window_size_spin = QSpinBox()
        self.window_size_spin.setRange(1, 999999)
        self.window_size_spin.setMaximum(999999)
        self.window_size_spin.setValue(300)

        self.window_size_spin.setSingleStep(1)

        self.threshold_label = QLabel("Edge Threshold")
        self.threshold_spin = QSpinBox()
        self.threshold_spin.setValue(20)
        self.threshold_spin.setRange(0, 1000)
        self.threshold_spin.setSingleStep(1)

        self.start_button = QPushButton("Start")
        self.stop_button = QPushButton("Stop")
        self.output_button = QPushButton("Display Output")

        layout.addWidget(self.input_file_label, 0, 0)
        layout.addWidget(self.input_file_edit, 0, 1)
        layout.addWidget(self.browse_button, 0, 2)
        layout.addWidget(self.line_count_label, 1, 0)
        layout.addWidget(self.line_count_edit, 1, 1)
        layout.addWidget(self.window_size_label, 2, 0)
        layout.addWidget(self.window_size_spin, 2, 1)
        layout.addWidget(self.threshold_label, 3, 0)
        layout.addWidget(self.threshold_spin, 3, 1)
        layout.addWidget(self.start_button, 4, 0)
        layout.addWidget(self.stop_button, 4, 1)
        layout.addWidget(self.output_button, 5, 0)

        self.connectSlots()

    def connectSlots(self):
        """
        Connects signals to slots.
        """
        self.connect(self.browse_button, SIGNAL('clicked()'), self.browseFile)
        self.connect(self.stop_button, SIGNAL('clicked()'), self.stop)
        self.connect(self.start_button, SIGNAL('clicked()'), self.start)
        #self.connect(self.new_job_button, SIGNAL('clicked()'), self.start_new_nob)

    def browseFile(self):
        self.input_file_edit.setText(QFileDialog.getOpenFileName())

    def start(self):
        filename = str(self.input_file_edit.text())
        item_count = int(self.line_count_edit.text())
        window_size = int(self.window_size_spin.text())
        prune_threshold = int(self.threshold_spin.text())
        generate_graph = False

        params = (filename, item_count, window_size, prune_threshold, generate_graph)
        app = ZarganApp(*params)
        self.computation = Process(target=app.run, args=())
        self.computation.start()

    def stop(self):
        logger.info("Terminating the computation...")
        self.computation.terminate()

    def generate_graph(self):
        app.generate_graph()
        app.write_graph()

    def showStatusMessage(self, message):
        self.statusBar().showMessage(message)

def main():
    app = QApplication(sys.argv)
    mainWindow = MainWindow()
    mainWindow.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()