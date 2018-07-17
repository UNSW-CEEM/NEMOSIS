#!/usr/bin/python3
# -*- coding: utf-8 -*-

import rows
import defaults
import maps
import pandas as pd
import tkinter as tk
import tkinter.ttk as ttk
from tkinter import filedialog
import pickle


class VerticalScrollFrame(ttk.Frame):
    """A ttk frame allowing vertical scrolling only.
    Use the '.interior' attribute to place widgets inside the scrollable frame.
    Adapted from https://github.com/sunbearc22/tkinterWidgets/blob/master/scrframe.py.
    """

    def __init__(self, parent, *args, **options):
        mainborderwidth = options.pop('mainborderwidth', 0)
        interiorborderwidth = options.pop('interiorborderwidth', 0)
        mainrelief = options.pop('mainrelief', 'flat')
        interiorrelief = options.pop('interiorrelief', 'flat')
        ttk.Frame.__init__(self, parent, style='main.TFrame', borderwidth=mainborderwidth, relief=mainrelief)
        self.__createWidgets(interiorborderwidth, interiorrelief)
        self.canvas.bind('<Configure>', self.update_scrollbar)

    def __createWidgets(self, interiorborderwidth, interiorrelief):
        self.vscrollbar = ttk.Scrollbar(self, orient='vertical',style='canvas.Vertical.TScrollbar')
        self.vscrollbar.pack(side='right', fill='y', expand='false')
        self.canvas = tk.Canvas(self, yscrollcommand=self.vscrollbar.set)
        self.canvas.pack(side='left', fill='both', expand='true')
        self.vscrollbar.config(command=self.canvas.yview)

        # reset the view
        self.canvas.xview_moveto(0)
        self.canvas.yview_moveto(0)

        # create a frame inside the canvas which will be scrolled with it
        self.interior = ttk.Frame(self.canvas, borderwidth=interiorborderwidth, relief=interiorrelief)
        self.interior_id = self.canvas.create_window(0, 0, window=self.interior, anchor='nw')

    def update_scrollbar(self, event):
        '''Configure the interior frame size and the canvas scrollregion'''
        # Force the update of .winfo_width() and winfo_height()
        self.canvas.update_idletasks()

        # Internal parameters
        interiorReqHeight = self.interior.winfo_reqheight()
        canvasWidth = self.canvas.winfo_width()
        canvasHeight = self.canvas.winfo_height()

        # Set interior frame width to canvas current width
        self.canvas.itemconfigure(self.interior_id, width=canvasWidth)

        # Set interior frame height and canvas scrollregion
        if canvasHeight > interiorReqHeight:
            self.canvas.itemconfigure(self.interior_id, height=canvasHeight)
            self.canvas.config(scrollregion="0 0 {0} {1}".
                               format(canvasWidth, canvasHeight))
        else:
            self.canvas.itemconfigure(self.interior_id, height=interiorReqHeight)
            self.canvas.config(scrollregion="0 0 {0} {1}".
                               format(canvasWidth, interiorReqHeight))


class App(ttk.Frame):

    def __init__(self, parent, *args, **kwargs):

        ttk.Frame.__init__(self, parent=None, style='App.TFrame', borderwidth=0, relief='raised', width=890, height=590)
        self.parent = parent
        self.parent.title('CEEM NEM Data Access Tool')
        self.parent.geometry('900x550')
        self.setStyle()
        self.createWidgets()
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)

    def setStyle(self):
        style = ttk.Style()
        style.configure('App.TFrame', background='pink')

    def createWidgets(self):
        self.frame = VerticalScrollFrame(self, arrowcolor='white',  mainborderwidth=10, interiorborderwidth=10,
                                         mainrelief='raised', interiorrelief='sunken')

        self.frame.grid(row=0, column=0, sticky='nsew')
        self.rows = []
        self.add_header()
        self.add_plus()
        self.add_query()

    def add_header(self):
        # Create the default starting widgets that appear at the top of the gui.

        # Button to run the app.
        self.run = ttk.Button(self.frame.interior, text='Run queries', command=self.run_queries)
        self.run.grid(row=0, column=0)
        self.run.config(width=20)
        self.run.update()

        # Label for save location entry box.
        self.save_label = tk.Label(self.frame.interior, text='  Output data to:', anchor='w')
        self.save_label.grid(row=0, column=1)
        self.save_label.config(width=15)
        self.save_label.update()

        # Text entry that specifies the location to save query results.
        self.save_location = ttk.Entry(self.frame.interior)
        self.save_location.grid(row=0, column=2, columnspan=defaults.save_field_column_span)
        self.save_location.config(width=50)
        self.save_location.update()

        # Label for the raw data location entry box.
        self.raw_data_label = ttk.Label(self.frame.interior, text='Raw data cache:', anchor='w')
        self.raw_data_label.grid(row=1, column=1)
        self.raw_data_label.config(width=15)
        self.raw_data_label.update()

        # Text entry that specifies the location of the raw aemo data cache.
        self.raw_data_location = ttk.Entry(self.frame.interior)
        self.raw_data_location.grid(row=1, column=2, columnspan=defaults.save_field_column_span)
        self.raw_data_location.config(width=50)
        self.raw_data_location.update()

        # Button to save current state of the gui.
        self.save = ttk.Button(self.frame.interior, text='Save session', command=self.save_session)
        self.save.grid(row=0, column=5, padx = 10)
        self.save.config(width=20)
        self.save.update()

        # Button to load a previous state of the gui.
        self.load = ttk.Button(self.frame.interior, text='Load session', command=self.load_session)
        self.load.grid(row=1, column=5, padx = 10)
        self.load.config(width=20)
        self.load.update()

    def add_plus(self):
        # Add the button that added extra query and merge rows to the gui.

        # Button to add extra queries.
        self.plus_query = ttk.Button(self.frame.interior, text=u"\u2795" + ' Query', command=self.add_query, width=10)
        self.plus_query.grid(row=defaults.query_row_offset + len(self.rows) * defaults.row_height
                            + defaults.plus_internal_row, column=0, padx=defaults.standard_x_pad,sticky='w')
        self.plus_query.update()

        # Button to add extra merge.
        self.plus_merge = ttk.Button(self.frame.interior, text=u"\u2795" + ' Merge', command=self.add_merge, width=10)
        self.plus_merge.grid(row=defaults.query_row_offset + len(self.rows) * defaults.row_height
                            + defaults.plus_merge_internal_row, column=0, padx=defaults.standard_x_pad, sticky='w')
        self.plus_merge.update()

    def add_query(self):
        # Function to add extra query.
        self.rows.append(rows.Query(self.frame.interior, len(self.rows), self))
        self.replace_plus()

    def add_merge(self):
        # Function to add extra merge.
        self.rows.append(rows.Merge(self.frame.interior, len(self.rows), self))
        self.replace_plus()

    def delete_row(self, row_number):
        # Function to delete row.
        # Remove widgets and delete row.
        self.rows[row_number].empty()
        del self.rows[row_number]
        # Reposition remaining rows of queries and merges so no gaps are left.
        for row_number, row in enumerate(self.rows):
            row.row_number = row_number
            row.position()
        self.frame.update_scrollbar(None)

    def run_queries(self):
        # Run all the rows in the gui.
        results = {}
        save_location = self.save_location.get()
        raw_data_location = self.raw_data_location.get()
        for row in self.rows:
            save_name = row.name.get()
            if type(row).__name__ == 'Query':
                results[save_name] = self.run_query(row, raw_data_location)
            elif type(row).__name__ == 'Merge':
                results[save_name] = self.run_merge(row, results)
            results[save_name].to_csv(save_location + '\\' + save_name + '.csv', index=False,
                                      date_format='%Y/%m/%d %H:%M:%S')

        return

    def run_query(self, row, raw_data_location):
        # Run an individual query.

        # Find the table name from the row.
        table = defaults.return_tables[row.tables.curselection()[0]]
        # Find the select columns.
        columns = tuple([row.col_list.get(0, tk.END)[index] for index in row.col_list.curselection()])
        # Find the columns that could be filtered on.
        potential_filter_cols = list(row.filter_label.keys())
        filter_cols = ()
        filter_values = ()
        # Build a list of filter columns and filter values if the filters list have any values in them.
        for column in potential_filter_cols:
            selection = [row.filter_list[column].get(0, tk.END)[index] for index in
                         row.filter_list[column].curselection()]
            if len(selection) > 0:
                    filter_values = filter_values + (selection,)
                    filter_cols = filter_cols + (column,)

        start_time = row.start_time.get()
        end_time = row.end_time.get()
        # Call the query using the tables predefined wraper function.
        result = maps.map[table](start_time, end_time, table, raw_data_location, columns, filter_cols,
                                 filter_values)
        return result

    def run_merge(self, row, results):
        # Run an individual merge row.
        # Get the name of the result to put on the left of the merge.
        left_table_name = row.left_table.get()
        # Get the result to put on the left of the merge.
        left_table = results[left_table_name]
        # Get the keys to use on the right result.
        left_keys = [row.left_key_list.get(0, tk.END)[index] for index in row.left_key_list.curselection()]
        # Get the name of the result to put on the left of the merge.
        right_table_name = row.right_table.get()
        # Get the result to put on the right of the merge.
        right_table = results[right_table_name]
        # Get the keys to use on the right result.
        right_keys = [row.right_key_list.get(0, tk.END)[index] for index in row.right_key_list.curselection()]
        # Get the join type to use.
        join_type = defaults.join_type[row.join_types.curselection()[0]]
        # Merge the results.
        result = pd.merge(left_table, right_table, join_type, left_on=left_keys, right_on=right_keys)
        return result

    def save_session(self):
        # Save the current state of the gui and pickle the result.
        session_state = {}
        session_state['raw_data_location'] = self.raw_data_location.get()
        session_state['save_location'] = self.save_location.get()
        session_state['rows'] = []
        for row in self.rows:
            session_state['rows'].append(row.state())
        save_name = filedialog.asksaveasfilename()

        # If a user provides a save name with .pkl already in it do not add another .pkl
        if save_name[-4:] != '.pkl':
            save_name = save_name + '.pkl'

        with open(save_name, 'wb') as f:
            pickle.dump(session_state, f, pickle.HIGHEST_PROTOCOL)

    def load_session(self, session=None):
        # Reconfigure the gui to match a state defined in a dictionary.

        # If the function is called with a session argument then use that (this is used for testing). If it is called
        # without a session argument then a popup asks the user to provide one.
        if session is None:
            save_name = filedialog.askopenfilename()
            with open(save_name, 'rb') as f:
                session_state = pickle.load(f)
        else:
            session_state = session

        # Load in raw data and save locations.
        self.raw_data_location.delete(0, 'end')
        self.raw_data_location.insert(0, session_state['raw_data_location'])
        self.save_location.delete(0, 'end')
        self.save_location.insert(0, session_state['save_location'])

        # Empty any existing rows.
        for row in self.rows:
            row.empty()
        # Create and empty row list to delete any existing rows.
        self.rows = []

        # Create rows based on the saved session.
        for row in session_state['rows']:
            # Create the right type of row.
            if row['type'] == 'query':
                self.rows.append(rows.Query(self.frame.interior, len(self.rows), self))
            elif row['type'] == 'merge':
                self.rows.append(rows.Merge(self.frame.interior, len(self.rows), self))
            # Load the row state.
            self.rows[-1].load_state(row)

        # Put the plus button at the buttom of all the rows.
        self.replace_plus()


    def replace_plus(self):
        # Move the plus buttons to below all existing rows.
        self.plus_query.grid(row=defaults.query_row_offset + (len(self.rows)) * defaults.row_height
                            + defaults.plus_internal_row)
        self.plus_merge.grid(row=defaults.query_row_offset + (len(self.rows)) * defaults.row_height
                            + defaults.plus_merge_internal_row)
        self.plus_query.update()
        self.plus_merge.update()
        self.frame.update_scrollbar(None)


if __name__ == '__main__':
    root = tk.Tk()
    app = App(root)
    app.grid(row=0, column=0, sticky='nsew')
    root.rowconfigure(0, weight=1)
    root.columnconfigure(0, weight=1)
    root.mainloop()