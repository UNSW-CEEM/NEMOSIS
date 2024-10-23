from nemosis import rows, defaults, data_fetch_methods
import pandas as pd
import tkinter as tk
import tkinter.ttk as ttk
from tkinter import messagebox
from tkinter import filedialog
import pickle
import traceback
import os
import sys
from pathlib import Path


class VerticalScrollFrame(ttk.Frame):
    """A ttk frame allowing vertical scrolling only.
    Adapted from https://github.com/sunbearc22/tkinterWidgets/blob/master/scrframe.py.
    """

    def __init__(self, parent, *args, **options):
        mainborderwidth = options.pop("mainborderwidth", 0)
        interiorborderwidth = options.pop("interiorborderwidth", 0)
        mainrelief = options.pop("mainrelief", "flat")
        interiorrelief = options.pop("interiorrelief", "flat")
        ttk.Frame.__init__(
            self,
            parent,
            style="main.TFrame",
            borderwidth=mainborderwidth,
            relief=mainrelief,
        )
        self.__createWidgets(interiorborderwidth, interiorrelief)
        self.canvas.bind("<Configure>", self.update_scrollbar)

    def __createWidgets(self, interiorborderwidth, interiorrelief):
        self.vscrollbar = ttk.Scrollbar(
            self, orient="vertical", style="canvas.Vertical.TScrollbar"
        )
        self.vscrollbar.pack(side="right", fill="y", expand="false")
        self.canvas = tk.Canvas(
            self, yscrollcommand=self.vscrollbar.set, highlightthickness=0
        )
        self.canvas.pack(side="left", fill="both", expand="true")
        self.vscrollbar.config(command=self.canvas.yview)

        # reset the view
        self.canvas.xview_moveto(0)
        self.canvas.yview_moveto(0)

        # create a frame inside the canvas which will be scrolled with it
        self.interior = ttk.Frame(
            self.canvas, borderwidth=interiorborderwidth, relief=interiorrelief
        )
        self.interior_id = self.canvas.create_window(
            0,
            0,
            window=self.interior,
            anchor="nw",
        )

    def update_scrollbar(self, event):
        """Configure the interior frame size and the canvas scrollregion"""
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
            self.canvas.config(
                scrollregion="0 0 {0} {1}".format(canvasWidth, canvasHeight)
            )
        else:
            self.canvas.itemconfigure(self.interior_id, height=interiorReqHeight)
            self.canvas.config(
                scrollregion="0 0 {0} {1}".format(canvasWidth, interiorReqHeight)
            )


class App(ttk.Frame):
    def __init__(self, parent, *args, **kwargs):

        ttk.Frame.__init__(
            self, parent=None, style="App.TFrame", borderwidth=0, width=890, height=590
        )
        self.parent = parent
        self.parent.title("NEMOSIS")
        self.parent.geometry("1000x600")
        self.setStyle()
        self.createWidgets()
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)

    def setStyle(self):
        style = ttk.Style()
        style.configure("App.TFrame", background="pink")

    def createWidgets(self):
        self.frame = VerticalScrollFrame(
            self,
            arrowcolor="white",
            mainborderwidth=10,
            interiorborderwidth=10,
            mainrelief="raised",
            interiorrelief="sunken",
        )

        self.frame.grid(row=0, column=0, sticky="nsew")
        self.rows = []
        self.add_header()
        self.add_plus()
        self.add_AEMO_query()

    def add_header(self):
        # Create the default starting widgets that appear at the top of the gui.
        self.header = ttk.Frame(self.frame.interior)
        self.header.grid(row=0, column=0, columnspan=50, sticky="w")
        self.header.update()

        # Label for save location entry box.
        self.save_label = tk.Label(self.header, text="  Output data to:", anchor="w")
        self.save_label.grid(row=0, column=1)
        self.save_label.config(width=15)
        self.save_label.update()

        # Text entry that specifies the location to save query results.
        self.save_location = ttk.Entry(self.header)
        self.save_location.grid(
            row=0, column=2, columnspan=defaults.save_field_column_span
        )
        self.save_location.config(width=50)
        self.save_location.update()

        # Button set save location.
        self.output_location = ttk.Button(
            self.header, text="...", command=self.set_save_location
        )
        self.output_location.grid(row=0, column=5)
        self.output_location.config(width=4)
        self.output_location.update()

        # Label for the raw data location entry box.
        self.raw_data_label = ttk.Label(self.header, text="Raw data cache:", anchor="w")
        self.raw_data_label.grid(row=1, column=1)
        self.raw_data_label.config(width=15)
        self.raw_data_label.update()

        # Text entry that specifies the location of the raw aemo data cache.
        self.raw_data_location = ttk.Entry(self.header)
        self.raw_data_location.grid(
            row=1, column=2, columnspan=defaults.save_field_column_span
        )
        self.raw_data_location.config(width=50)
        self.raw_data_location.update()

        # Button set save location.
        self.output_location = ttk.Button(
            self.header, text="...", command=self.set_cache_location
        )
        self.output_location.grid(row=1, column=5)
        self.output_location.config(width=4)
        self.output_location.update()

        # Button to save current state of the gui.
        self.save = ttk.Button(
            self.header, text="Save session", command=self.save_session
        )
        self.save.grid(row=0, column=6, padx=20)
        self.save.config(width=20)
        self.save.update()

        # Button to load a previous state of the gui.
        self.load = ttk.Button(
            self.header, text="Load session", command=self.load_session
        )
        self.load.grid(row=1, column=6, padx=20)
        self.load.config(width=20)
        self.load.update()

    def add_plus(self):
        # Add the button that added extra query and merge rows to the gui.
        self.row_adder = ttk.Frame(self.frame.interior)
        self.row_adder.grid(
            row=defaults.query_row_offset
                + len(self.rows) * defaults.row_height
                + defaults.plus_internal_row,
            column=0,
            padx=defaults.standard_x_pad,
            sticky="w",
            columnspan=50,
            pady=10,
        )
        self.row_adder.update()

        # Button to add extra queries.
        self.plus_AEMO_query = ttk.Button(
            self.row_adder, text="\u2795" + " AEMO table", command=self.add_AEMO_query
        )
        self.plus_AEMO_query.grid(row=0, column=0)
        self.plus_AEMO_query.update()

        # Button to add extra queries.
        self.plus_custom_query = ttk.Button(
            self.row_adder,
            text="\u2795" + " Custom table",
            command=self.add_Custom_query,
        )
        self.plus_custom_query.grid(row=0, column=1)
        self.plus_custom_query.update()

        # Button to add extra merge.
        self.plus_merge = ttk.Button(
            self.row_adder, text="\u2795" + " Merge", command=self.add_merge
        )
        self.plus_merge.grid(row=0, column=3)
        self.plus_merge.update()

        # Button to add extra merge.
        self.plus_merge_as_of = ttk.Button(
            self.row_adder,
            text="\u2795" + " Merge on most recent  ",
            command=self.add_merge_as_of,
        )
        self.plus_merge_as_of.grid(row=0, column=4)
        self.plus_merge_as_of.update()

        # Button to add extra filter version no.
        self.plus_filter_version_no = ttk.Button(
            self.row_adder,
            text="\u2795" + " Highest version No. ",
            command=self.add_filter_version_no,
        )
        self.plus_filter_version_no.grid(row=0, column=5)
        self.plus_filter_version_no.update()

        # Button to run the app.
        self.run = ttk.Button(
            self.row_adder, text="\u25B6" + "  Run queries ", command=self.run_queries
        )
        self.run.grid(row=0, column=6)
        self.run.update()

    def add_AEMO_query(self):
        # Function to add extra query.
        self.rows.append(
            rows.Query(
                self.frame.interior,
                len(self.rows),
                self,
                table_options=defaults.display_as_AMEO,
            )
        )
        self.replace_plus()

    def add_Custom_query(self):
        # Function to add extra query.
        self.rows.append(
            rows.Query(
                self.frame.interior,
                len(self.rows),
                self,
                table_options=defaults.display_as_Custom,
            )
        )
        self.replace_plus()

    def add_merge(self):
        # Function to add extra merge.
        self.rows.append(rows.Merge(self.frame.interior, len(self.rows), self))
        self.replace_plus()

    def add_merge_as_of(self):
        # Function to add extra merge.
        self.rows.append(rows.Merge_as_of(self.frame.interior, len(self.rows), self))
        self.replace_plus()

    def add_filter_version_no(self):
        # Function to add extra merge.
        self.rows.append(
            rows.FilterVersionNo(self.frame.interior, len(self.rows), self)
        )
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
        try:
            Path(save_location).mkdir(parents=False, exist_ok=True)
            Path(raw_data_location).mkdir(parents=False, exist_ok=True)
            for row in self.rows:
                save_name = row.name.get()
                if type(row).__name__ == "Query":
                    results[save_name] = self.run_query(row, raw_data_location)
                elif type(row).__name__ == "Merge":
                    results[save_name] = self.run_merge(row, results)
                elif type(row).__name__ == "Merge_as_of":
                    results[save_name] = self.run_merge_as_of(row, results)
                elif type(row).__name__ == "FilterVersionNo":
                    results[save_name] = self.run_filter_version_no(row, results)

                results[save_name].to_csv(
                    Path(save_location) / (save_name + ".csv"),
                    index=False,
                    date_format="%Y/%m/%d %H:%M:%S",
                )
            messagebox.showinfo("Finished", "Your query has finished!")
        except Exception:
            traceback.print_exc()
            messagebox.showerror(
                "Error",
                "Your query executed with an error. "
                "\nReview the console for detailed information",
            )

        return

    def run_query(self, row, raw_data_location):
        # Run an individual query.

        # Find the table name from the row.
        table = row.table_options[row.tables.curselection()[0]]
        # Find the select columns.
        columns = tuple(
            [
                row.col_list.get(0, tk.END)[index]
                for index in row.col_list.curselection()
            ]
        )
        # Find the columns that could be filtered on.
        potential_filter_cols = list(row.filter_label.keys())
        filter_cols = ()
        filter_values = ()
        # Build a list of filter columns and filter values if the filters list have any values in them.
        for column in potential_filter_cols:
            selection = [
                row.filter_list[column].get(0, tk.END)[index]
                for index in row.filter_list[column].curselection()
            ]
            if len(selection) > 0:
                filter_values = filter_values + (selection,)
                filter_cols = filter_cols + (column,)

        start_time = row.start_time.get()
        end_time = row.end_time.get()
        # Call the query using the tables predefined wraper function.
        result = data_fetch_methods._method_map[table](
            start_time,
            end_time,
            table,
            raw_data_location,
            columns,
            filter_cols,
            filter_values,
        )
        return result

    def run_merge(self, row, results):
        # Run an individual merge row.
        # Get the name of the result to put on the left of the merge.
        left_table_name = row.left_table.get()
        # Get the result to put on the left of the merge.
        left_table = results[left_table_name]
        # Get the keys to use on the right result.
        left_keys = [
            row.left_key_list.get(0, tk.END)[index]
            for index in row.left_key_list.curselection()
        ]
        # Get the name of the result to put on the left of the merge.
        right_table_name = row.right_table.get()
        # Get the result to put on the right of the merge.
        right_table = results[right_table_name]
        # Get the keys to use on the right result.
        right_keys = [
            row.right_key_list.get(0, tk.END)[index]
            for index in row.right_key_list.curselection()
        ]
        # Get the join type to use.
        join_type = defaults.join_type[row.join_types.curselection()[0]]
        # Merge the results.
        result = pd.merge(
            left_table, right_table, join_type, left_on=left_keys, right_on=right_keys
        )
        return result

    def run_merge_as_of(self, row, results):
        # Run an individual merge row.
        # Get the name of the result to put on the left of the merge.
        left_table_name = row.left_table.get()
        # Get the result to put on the left of the merge.
        left_table = results[left_table_name]
        # Get the keys to use on the right result.
        left_keys = [
            row.left_key_list.get(0, tk.END)[index]
            for index in row.left_key_list.curselection()
        ]
        # Get the keys to use on the right result.
        left_time_keys = [
            row.left_time_key_list.get(0, tk.END)[index]
            for index in row.left_time_key_list.curselection()
        ]
        # Get the name of the result to put on the left of the merge.
        right_table_name = row.right_table.get()
        # Get the result to put on the right of the merge.
        right_table = results[right_table_name]
        # Get the keys to use on the right result.
        right_keys = [
            row.right_key_list.get(0, tk.END)[index]
            for index in row.right_key_list.curselection()
        ]
        # Get the keys to use on the right result.
        right_time_keys = [
            row.right_time_key_list.get(0, tk.END)[index]
            for index in row.right_time_key_list.curselection()
        ]
        # Get the join type to use.
        join_type = defaults.join_type[row.join_types.curselection()[0]]
        # Merge the results.
        left_time_key = left_time_keys[0]
        right_time_key = right_time_keys[0]
        left_table[left_time_key] = pd.to_datetime(left_table[left_time_key])
        right_table[right_time_key] = pd.to_datetime(right_table[right_time_key])
        left_table = left_table.sort_values(left_time_key)
        right_table = right_table.sort_values(right_time_key)
        result = pd.merge_asof(
            left_table,
            right_table,
            left_on=left_time_key,
            right_on=right_time_key,
            left_by=left_keys,
            right_by=right_keys,
        )
        return result

    def run_filter_version_no(self, row, results):
        input_name = row.input.get()
        input_table = results[input_name]
        group_cols = []
        for key_set in defaults.table_primary_keys.values():
            group_cols += [
                col
                for col in key_set
                if (
                    (col in input_table.columns)
                    & (col != "VERSIONNO")
                    & (col not in group_cols)
                )
            ]
        input_table = input_table.sort_values("VERSIONNO")
        result = input_table.groupby(by=group_cols, as_index=False).last()
        return result

    def set_save_location(self):
        save_name = filedialog.askdirectory()
        self.save_location.delete(0, "end")
        self.save_location.insert("end", save_name)

    def set_cache_location(self):
        save_name = filedialog.askdirectory()
        self.raw_data_location.delete(0, "end")
        self.raw_data_location.insert("end", save_name)

    def save_session(self):
        # Save the current state of the gui and pickle the result.
        session_state = {}
        session_state["raw_data_location"] = self.raw_data_location.get()
        session_state["save_location"] = self.save_location.get()
        session_state["rows"] = []
        for row in self.rows:
            session_state["rows"].append(row.state())
        save_name = filedialog.asksaveasfilename()

        # If a user provides a save name with .pkl already in it do not add another .pkl
        if save_name[-4:] != ".pkl":
            save_name = save_name + ".pkl"

        with open(save_name, "wb") as f:
            pickle.dump(session_state, f, pickle.HIGHEST_PROTOCOL)

    def load_session(self, session=None):
        # Reconfigure the gui to match a state defined in a dictionary.

        # If the function is called with a session argument then use that (this is used for testing). If it is called
        # without a session argument then a popup asks the user to provide one.
        if session is None:
            save_name = filedialog.askopenfilename()
            with open(save_name, "rb") as f:
                session_state = pickle.load(f)
        else:
            session_state = session

        # Load in raw data and save locations.
        self.raw_data_location.delete(0, "end")
        self.raw_data_location.insert(0, session_state["raw_data_location"])
        self.save_location.delete(0, "end")
        self.save_location.insert(0, session_state["save_location"])

        # Empty any existing rows.
        for row in self.rows:
            row.empty()
        # Create and empty row list to delete any existing rows.
        self.rows = []

        # Create rows based on the saved session.
        for row in session_state["rows"]:
            # Create the right type of row.
            if row["type"] == "query":
                if len(row["table"]) != 0:
                    if row["table"] in defaults.display_as_AMEO:
                        self.rows.append(
                            rows.Query(
                                self.frame.interior,
                                len(self.rows),
                                self,
                                defaults.display_as_AMEO,
                            )
                        )
                    if row["table"] in defaults.display_as_Custom:
                        self.rows.append(
                            rows.Query(
                                self.frame.interior,
                                len(self.rows),
                                self,
                                defaults.display_as_Custom,
                            )
                        )

            elif row["type"] == "merge":
                self.rows.append(rows.Merge(self.frame.interior, len(self.rows), self))
            elif row["type"] == "merge_as_of":
                self.rows.append(
                    rows.Merge_as_of(self.frame.interior, len(self.rows), self)
                )
            elif row["type"] == "filter_version_no":
                self.rows.append(
                    rows.FilterVersionNo(self.frame.interior, len(self.rows), self)
                )
            # Load the row state.
            self.rows[-1].load_state(row)

        # Put the plus button at the bottom of all the rows.
        self.replace_plus()

    def replace_plus(self):
        # Move the plus buttons to below all existing rows.
        self.row_adder.grid(
            row=defaults.query_row_offset
                + (len(self.rows)) * defaults.row_height
                + defaults.plus_internal_row
        )
        self.row_adder.update()
        self.frame.update_scrollbar(None)


def resource_path(relative_path):
    """Get absolute path to resource, works for dev and for PyInstaller"""
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath("")

    return os.path.join(base_path, relative_path)


if __name__ == "__main__":
    import tempfile

    ICON = (
        (
            b"\x00\x00\x01\x00\x01\x00\x10\x10\x00\x00\x01\x00\x08\x00h\x05\x00\x00"
            b"\x16\x00\x00\x00(\x00\x00\x00\x10\x00\x00\x00 \x00\x00\x00\x01\x00"
            b"\x08\x00\x00\x00\x00\x00@\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
            b"\x00\x01\x00\x00\x00\x01"
        )
        + b"\x00" * 1282
        + b"\xff" * 64
    )

    _, ICON_PATH = tempfile.mkstemp()
    with open(ICON_PATH, "wb") as icon_file:
        icon_file.write(ICON)

    root = tk.Tk()
    app = App(root)
    app.grid(row=0, column=0, sticky="nsew")
    root.rowconfigure(0, weight=1)
    root.columnconfigure(0, weight=1)
    root.iconbitmap(resource_path("favicon.ico"))
    root.mainloop()
